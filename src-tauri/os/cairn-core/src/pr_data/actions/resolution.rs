//! PR-node resolution: the merge / close lifecycle (`resolve_pr_node`), the
//! `merge_requests` status transitions, and downstream execution advancement.

use crate::issues::status::SettlementReport;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use crate::transitions::Resolution;
use cairn_db::turso::params;

use super::context::{
    create_pr_artifact_job_for_merge_request, db_error, resolve_merge_mr_context_for_job,
    MergeMrContext, MrContext, PrNodeResolution,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrResolutionAttribution {
    pub actor_kind: &'static str,
    pub actor_identity: Option<String>,
    pub surface: &'static str,
}

pub struct StoredPrResolutionAttribution {
    pub action: String,
    pub actor_kind: String,
    pub actor_identity: Option<String>,
    pub surface: String,
    pub created_at: i64,
    pub lane_snapshot: LaneSnapshot,
}

/// The state of the work a pull request's resolution was made against: each
/// check lane's verdict, the producing execution's jobs, and the head the pull
/// request stood at. It is what shows, afterwards, that a merge landed with
/// lanes still waiting for a verdict.
///
/// The execution is named, never copied. Its snapshot (every agent's prompt,
/// the recipe graph, every skill) runs to hundreds of kilobytes and lives on
/// the execution's own resource.
///
/// Rows journaled before this shape also carry `execution.executionSnapshot`
/// and each lane's full status (output tails, failure names). [`Self::parse`]
/// reads both shapes and keeps only these fields. A failed lane's failing
/// tests are read through its cited observation when the row is rendered.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LaneSnapshot {
    pub execution: Option<ExecutionAtResolution>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
    pub systematic_checks: Vec<LaneAtResolution>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionAtResolution {
    pub id: Option<String>,
    pub execution_status: Option<String>,
    pub jobs: Vec<JobAtResolution>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct JobAtResolution {
    #[serde(default)]
    pub node: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
}

/// One check lane as it stood at the resolution. `state` is the lane's
/// [`NodeCheckState`](crate::execution::checks_status::NodeCheckState) word;
/// `pending` and `running` mean the lane had no verdict yet, and
/// `notApplicable` that the impact gate excluded it from the judged change.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LaneAtResolution {
    pub name: String,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passed: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skipped: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ran_at: Option<i64>,
    /// The earlier head of the branch the verdict was recorded against, when
    /// it is not the head the pull request stood at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_on: Option<LaneHead>,
    /// Where the lane's evidence lives. `None` only on rows journaled before
    /// lanes cited their observations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation: Option<LaneObservation>,
    /// What the cited observation records about a failed verdict. Read when
    /// the resolution is rendered ([`hydrate_lane_failures`]), never stored.
    #[serde(skip)]
    pub failure: Option<ObservedFailure>,
    /// The review wave attempt this resolution cancelled while the lane was
    /// still waiting on it. Absent on rows journaled before it was recorded,
    /// which therefore cannot say whether their resolution cancelled a wave.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancelled_by: Option<String>,
}

/// Why a lane the resolution's own cascade cancelled has no observation.
const CANCELLED_BY_RESOLUTION: &str = "cancelled by this resolution";

/// A failed verdict as its observation holds it: the failure kind and the
/// first failing test names, with how many failed in all as the lane counted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObservedFailure {
    pub kind: Option<String>,
    pub names: Vec<String>,
    pub total: usize,
}

/// The evidence behind a lane's state at resolution. A cited observation
/// holds what the lane record leaves out (output tail, per-test results), so a
/// reader can recover why a lane was red; an absent one says why there is
/// nothing to cite rather than leaving the pointer null.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum LaneObservation {
    Cited {
        id: String,
        /// The observation's resolvable address, when it has a public handle;
        /// observations recorded before handles existed are cited by id.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        uri: Option<String>,
    },
    Absent {
        reason: String,
    },
}

impl LaneObservation {
    fn cell(observation: Option<&Self>) -> String {
        match observation {
            Some(Self::Cited { uri: Some(uri), .. }) => uri.clone(),
            Some(Self::Cited { id, uri: None }) => format!("observation `{id}`"),
            Some(Self::Absent { reason }) => format!("none: {reason}"),
            None => "not recorded".to_string(),
        }
    }
}

/// Why a lane whose status names no observation has none to cite.
fn absent_observation_reason(state: &str) -> &'static str {
    match state {
        "pending" | "running" => "no verdict at resolution",
        "notApplicable" => "the check does not apply",
        "unsupported" => "this build cannot run the check",
        _ => "its verdict row cites no observation",
    }
}

async fn lane_observation(
    db: &LocalDb,
    observation_id: Option<&str>,
    state: &str,
) -> LaneObservation {
    let Some(id) = observation_id else {
        return LaneObservation::Absent {
            reason: absent_observation_reason(state).to_string(),
        };
    };
    LaneObservation::Cited {
        id: id.to_string(),
        uri: observation_facts(db, id).await.and_then(|facts| facts.uri),
    }
}

/// What one observation row says about itself, for a lane that cites it.
struct ObservationFacts {
    /// Its resolvable address, when it has a public handle.
    uri: Option<String>,
    failed: bool,
    failure_kind: Option<String>,
}

async fn observation_facts(db: &LocalDb, id: &str) -> Option<ObservationFacts> {
    db.query_opt(
        "SELECT p.key, o.public_handle, o.verdict, o.failure_kind
           FROM check_result_observations o
           JOIN projects p ON p.id = o.project_id
          WHERE o.id = ?1",
        (id.to_string(),),
        |row| {
            let key = row.text(0)?;
            Ok(ObservationFacts {
                uri: row
                    .opt_text(1)?
                    .map(|handle| format!("cairn://p/{key}/check-observations/{handle}")),
                failed: row.text(2)? == "failed",
                failure_kind: row.opt_text(3)?,
            })
        },
    )
    .await
    .ok()
    .flatten()
}

/// Fill each listed lane's citation from its observation: the address a row
/// journaled without one lacks and, for a failed verdict, the failure kind and
/// failing tests. Rows journaled before lanes cited observations, and lanes a
/// migration relinked, carry nothing else that says why a lane was red.
///
/// Bounded like the block it feeds: only the lanes the table lists are read,
/// a lane that was not red and already has its address is not read at all,
/// and only as many names are read as the block shows, across all lanes. How
/// many failed is the lane's own count, which the row kept.
async fn hydrate_lane_failures(db: &LocalDb, snapshot: &mut LaneSnapshot) {
    let mut names_left = PROVENANCE_LINE_NAMES;
    for lane in snapshot
        .systematic_checks
        .iter_mut()
        .take(PROVENANCE_LANE_ROWS)
    {
        // Only a lane that was red at resolution shows failures; a pending or
        // running one shows no result, stale or not.
        let red = lane.state == "failed";
        let counted = lane.failed;
        let Some(LaneObservation::Cited { id, uri }) = &mut lane.observation else {
            continue;
        };
        if !red && uri.is_some() {
            continue;
        }
        let Some(facts) = observation_facts(db, id).await else {
            continue;
        };
        if uri.is_none() {
            *uri = facts.uri;
        }
        if !red || !facts.failed {
            continue;
        }
        let id = id.clone();
        let names = if names_left == 0 {
            Vec::new()
        } else {
            db.query_all(
                "SELECT test_id FROM check_test_results
                  WHERE observation_id = ?1 AND status = 'failed'
                  ORDER BY test_id LIMIT ?2",
                params![id.as_str(), names_left as i64],
                |row| row.text(0),
            )
            .await
            .unwrap_or_default()
        };
        names_left -= names.len().min(names_left);
        let total = counted.unwrap_or(0).max(names.len());
        lane.failure = Some(ObservedFailure {
            kind: facts.failure_kind,
            names,
            total,
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LaneHead {
    pub head: String,
}

impl LaneAtResolution {
    fn from_status(status: &crate::execution::checks_status::NodeCheckStatus) -> Option<Self> {
        // The lane's public projection is the one serialization of its state,
        // so the stored words are the ones every other surface shows.
        serde_json::to_value(status)
            .ok()
            .and_then(|value| serde_json::from_value(value).ok())
    }

    /// Whether the lane held a verdict when the pull request resolved.
    fn has_verdict(&self) -> bool {
        !matches!(self.state.as_str(), "pending" | "running")
    }

    fn verdict(&self) -> String {
        let mut state = match (self.state.as_str(), self.cancelled_by.as_deref()) {
            ("pending" | "running", Some(attempt)) => format!(
                "{}, no verdict; {CANCELLED_BY_RESOLUTION} (wave `{attempt}`)",
                self.state
            ),
            ("pending" | "running", None) => format!("{}, no verdict", self.state),
            ("notApplicable", _) => "not applicable".to_string(),
            (state, _) => state.to_string(),
        };
        if !self.has_verdict() {
            return state;
        }
        if let Some(kind) = self
            .failure
            .as_ref()
            .and_then(|failure| failure.kind.as_ref())
        {
            state.push_str(&format!(" ({kind})"));
        }
        let counts = [
            (self.passed, "passed"),
            (self.failed, "failed"),
            (self.skipped, "skipped"),
        ]
        .into_iter()
        .filter_map(|(count, label)| count.filter(|n| *n > 0).map(|n| format!("{n} {label}")))
        .collect::<Vec<_>>();
        if counts.is_empty() {
            state
        } else {
            format!("{state} ({})", counts.join(", "))
        }
    }
}

fn json_part<T: serde::de::DeserializeOwned>(value: &serde_json::Value, key: &str) -> Option<T> {
    value
        .get(key)
        .cloned()
        .and_then(|part| serde_json::from_value(part).ok())
}

fn json_list<T: serde::de::DeserializeOwned>(value: &serde_json::Value, key: &str) -> Vec<T> {
    value
        .get(key)
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| serde_json::from_value(item.clone()).ok())
                .collect()
        })
        .unwrap_or_default()
}

impl LaneSnapshot {
    /// Read a stored `lane_snapshot`, of either shape, field by field: a part
    /// that does not parse is absent rather than costing the rest.
    pub fn parse(stored: &str) -> Self {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(stored) else {
            return Self::default();
        };
        let execution = value
            .get("execution")
            .filter(|execution| execution.is_object())
            .map(|execution| ExecutionAtResolution {
                id: json_part(execution, "id"),
                execution_status: json_part(execution, "executionStatus"),
                jobs: json_list(execution, "jobs"),
            })
            .filter(|execution| {
                execution.id.is_some()
                    || execution.execution_status.is_some()
                    || !execution.jobs.is_empty()
            });
        Self {
            execution,
            head: json_part(&value, "head"),
            systematic_checks: json_list(&value, "systematicChecks"),
        }
    }
}

/// Where the rest of a resolution's provenance is read.
#[derive(Debug, Clone, Default)]
pub struct ProvenanceLinks {
    /// The producing node's `/checks`, which holds each lane's full detail.
    pub checks: Option<String>,
    /// The producing execution, which holds its snapshot.
    pub execution: Option<String>,
}

/// Lanes the provenance table lists before deferring to the checks link, so
/// the block stays a summary however many checks a project configures.
const PROVENANCE_LANE_ROWS: usize = 64;

/// Names a summary line lists before counting the rest. The lane table and
/// the linked resources carry every name; the cap keeps each line short
/// enough that a grep of the artifact returns readable lines.
const PROVENANCE_LINE_NAMES: usize = 5;

fn bounded_list(items: &[String]) -> String {
    let mut listed = items
        .iter()
        .take(PROVENANCE_LINE_NAMES)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    if items.len() > PROVENANCE_LINE_NAMES {
        listed.push_str(&format!(
            " and {} more",
            items.len() - PROVENANCE_LINE_NAMES
        ));
    }
    listed
}

/// Bytes of one failing test name a summary line shows.
const PROVENANCE_NAME_BYTES: usize = 160;

/// `text` cut to at most `bytes` on a character boundary, marked when cut.
fn excerpt(text: &str, bytes: usize) -> String {
    let end = text
        .char_indices()
        .map(|(offset, _)| offset)
        .find(|offset| *offset >= bytes)
        .unwrap_or(text.len());
    let mut shown = text[..end].to_string();
    if end < text.len() {
        shown.push('…');
    }
    shown
}

fn short_head(head: &str) -> &str {
    head.get(..12).unwrap_or(head)
}

fn table_cell(text: &str) -> String {
    text.replace('|', "\\|").replace('\n', " ")
}

/// The resolution line and its provenance block, as a PR artifact renders it:
/// who resolved it, on which surface, when, and a table of the check lanes as
/// they stood, with links to the detail.
pub fn render_resolution_provenance(
    attribution: &StoredPrResolutionAttribution,
    links: &ProvenanceLinks,
) -> String {
    let actor = resolution_actor_label(
        Some(attribution.actor_kind.as_str()),
        attribution.actor_identity.as_deref(),
        Some(attribution.surface.as_str()),
    );
    let verb = if attribution.action == "close" {
        "Closed"
    } else {
        "Merged"
    };
    let at = chrono::DateTime::from_timestamp(attribution.created_at, 0)
        .map(|time| time.to_rfc3339())
        .unwrap_or_else(|| attribution.created_at.to_string());
    let snapshot = &attribution.lane_snapshot;
    let mut out = format!(
        "{verb} by {actor} at {at}\n\n<details><summary>Resolution provenance</summary>\n\nSurface: `{}`\n",
        attribution.surface
    );
    if let Some(head) = snapshot.head.as_deref() {
        out.push_str(&format!("Head: `{}`\n", short_head(head)));
    }
    if let Some(execution) = &snapshot.execution {
        let mut line = format!(
            "Execution: {}",
            execution.execution_status.as_deref().unwrap_or("unknown")
        );
        let jobs = execution
            .jobs
            .iter()
            .map(|job| {
                format!(
                    "{} {}",
                    job.node.as_deref().unwrap_or("(unnamed)"),
                    job.status.as_deref().unwrap_or("unknown")
                )
            })
            .collect::<Vec<_>>();
        if !jobs.is_empty() {
            line.push_str(&format!("; jobs: {}", bounded_list(&jobs)));
        }
        out.push_str(&line);
        out.push('\n');
    }

    let lanes = &snapshot.systematic_checks;
    let tail = provenance_tail(lanes, links);
    if lanes.is_empty() {
        out.push_str("\nNo check lanes were recorded at resolution.\n");
    } else {
        let waiting = lanes
            .iter()
            .filter(|lane| !lane.has_verdict())
            .map(|lane| lane.name.clone())
            .collect::<Vec<_>>();
        if waiting.is_empty() {
            out.push_str(&format!(
                "\nEvery check lane ({}) had a verdict at resolution.\n",
                lanes.len()
            ));
        } else {
            out.push_str(&format!(
                "\n{} of {} check lanes had no verdict at resolution: {}.\n",
                waiting.len(),
                lanes.len(),
                bounded_list(&waiting)
            ));
        }
        if let Some(attempt) = lanes.iter().find_map(|lane| lane.cancelled_by.as_deref()) {
            let cancelled = lanes
                .iter()
                .filter(|lane| lane.cancelled_by.is_some())
                .map(|lane| lane.name.clone())
                .collect::<Vec<_>>();
            out.push_str(&format!(
                "This resolution cancelled review wave `{}` while these lanes waited on it: {}.\n",
                table_cell(attempt),
                bounded_list(&cancelled)
            ));
        }
        out.push_str("\n| Lane | Verdict | Head | Observation |\n|---|---|---|---|\n");
        let mut listed = 0;
        for lane in lanes.iter().take(PROVENANCE_LANE_ROWS) {
            let head = lane
                .verified_on
                .as_ref()
                .map(|verified| verified.head.as_str())
                .or(snapshot.head.as_deref())
                .map(|head| format!("`{}`", short_head(head)))
                .unwrap_or_else(|| "—".to_string());
            let row = format!(
                "| {} | {} | {head} | {} |\n",
                table_cell(&lane.name),
                table_cell(&lane.verdict()),
                table_cell(&LaneObservation::cell(lane.observation.as_ref()))
            );
            // Rows give way before the failures and links do: whatever is
            // left out is counted, and every row shown is whole.
            if out.len() + row.len() + OMITTED_LANES_LINE_BYTES + tail.len() >= PROVENANCE_BYTES {
                break;
            }
            out.push_str(&row);
            listed += 1;
        }
        if lanes.len() > listed {
            out.push_str(&format!(
                "\n{} more lanes are not listed.\n",
                lanes.len() - listed
            ));
        }
    }

    out.push_str(&tail);
    out
}

/// The byte budget the lane table's rows fill after the block's header and
/// tail are reserved: rows give way first, so the failing tests and links
/// still show however many lanes a project configures. The header and tail
/// are bounded by their own name caps, not by this budget.
const PROVENANCE_BYTES: usize = 8_192;

/// Room the lane table keeps for its `N more lanes are not listed.` line.
const OMITTED_LANES_LINE_BYTES: usize = 48;

/// What follows the lane table: the failing tests, the links, and the close.
fn provenance_tail(lanes: &[LaneAtResolution], links: &ProvenanceLinks) -> String {
    let mut out = String::new();
    // One budget across lanes bounds the failure detail of a very red merge.
    let shown = lanes
        .iter()
        .flat_map(|lane| {
            lane.failure
                .iter()
                .flat_map(move |failure| failure.names.iter().map(move |name| (lane, name)))
        })
        .take(PROVENANCE_LINE_NAMES)
        .collect::<Vec<_>>();
    for (lane, name) in &shown {
        out.push_str(&format!(
            "\nFailing test ({}): {}\n",
            table_cell(&excerpt(&lane.name, PROVENANCE_NAME_BYTES)),
            table_cell(&excerpt(name, PROVENANCE_NAME_BYTES))
        ));
    }
    let failed_tests = lanes
        .iter()
        .filter_map(|lane| lane.failure.as_ref())
        .map(|failure| failure.total.max(failure.names.len()))
        .sum::<usize>();
    if failed_tests > shown.len() {
        out.push_str(&format!(
            "\n{} more failing test names are not listed.\n",
            failed_tests - shown.len()
        ));
    }

    let mut rest = Vec::new();
    if let Some(checks) = &links.checks {
        rest.push(format!("lane detail: {checks}"));
    }
    if let Some(execution) = &links.execution {
        rest.push(format!("execution: {execution}"));
    }
    if !rest.is_empty() {
        out.push_str(&format!("\nThe rest: {}.\n", rest.join("; ")));
    }
    out.push_str("\n</details>\n");
    out
}

/// The execution that produced a pull request's owner: a producing job, or a
/// `pr` action run (CAIRN-1220).
async fn execution_id_for_owner(db: &LocalDb, owner_id: &str) -> Option<String> {
    db.query_opt(
        "SELECT COALESCE((SELECT execution_id FROM jobs WHERE id = ?1),
                         (SELECT execution_id FROM action_runs WHERE id = ?1))",
        (owner_id.to_string(),),
        |row| row.opt_text(0),
    )
    .await
    .ok()
    .flatten()
    .flatten()
}

/// The links a rendered provenance block offers for what it summarizes.
pub async fn provenance_links(
    db: &LocalDb,
    owner_id: &str,
    snapshot: &LaneSnapshot,
) -> ProvenanceLinks {
    let checks = crate::jobs::queries::home_uri_for_job(db, owner_id)
        .await
        .ok()
        .flatten()
        .map(|node| format!("{node}/checks"));
    let execution_id = match snapshot
        .execution
        .as_ref()
        .and_then(|execution| execution.id.clone())
    {
        Some(id) => Some(id),
        None => execution_id_for_owner(db, owner_id).await,
    };
    let execution = match execution_id {
        Some(id) => db
            .query_opt(
                "SELECT p.key, i.number, COALESCE(e.seq, 1)
                   FROM executions e
                   JOIN issues i ON i.id = e.issue_id
                   JOIN projects p ON p.id = i.project_id
                  WHERE e.id = ?1",
                (id,),
                |row| {
                    Ok(format!(
                        "cairn://p/{}/{}/{}",
                        row.text(0)?,
                        row.i64(1)?,
                        row.i64(2)?
                    ))
                },
            )
            .await
            .ok()
            .flatten(),
        None => None,
    };
    ProvenanceLinks { checks, execution }
}

impl PrResolutionAttribution {
    pub fn operator_ui() -> Self {
        Self {
            actor_kind: "operator-ui",
            actor_identity: None,
            surface: "operator-ui",
        }
    }
    pub fn operator_cli() -> Self {
        Self {
            actor_kind: "operator-cli",
            actor_identity: None,
            surface: "operator-cli",
        }
    }
    pub fn agent(uri: String) -> Self {
        Self {
            actor_kind: "agent",
            actor_identity: Some(uri),
            surface: "agent-resource-write",
        }
    }
    pub fn internal(path: &'static str, identity: impl Into<String>) -> Self {
        Self {
            actor_kind: "internal",
            actor_identity: Some(identity.into()),
            surface: path,
        }
    }
    /// A merge or close Cairn did not perform, seen on GitHub by a refresh.
    /// Nobody inside Cairn acted, so there is no principal to name.
    fn github_observation() -> Self {
        Self {
            actor_kind: "unjournaled",
            actor_identity: None,
            surface: GITHUB_OBSERVATION_SURFACE,
        }
    }
}

/// The surface a refresh journals when it observes a resolution Cairn did not
/// perform.
const GITHUB_OBSERVATION_SURFACE: &str = "github-refresh-observation";

/// Who a journaled resolution names, as the PR artifact and the issue summary
/// both render it.
pub fn resolution_actor_label(
    actor_kind: Option<&str>,
    actor_identity: Option<&str>,
    surface: Option<&str>,
) -> String {
    match (actor_kind, actor_identity) {
        (Some("operator-ui"), _) => "operator (UI)".to_string(),
        (Some("operator-cli"), _) => "operator (CLI)".to_string(),
        (Some("unjournaled"), _) if surface == Some(GITHUB_OBSERVATION_SURFACE) => {
            "someone outside Cairn (observed on GitHub)".to_string()
        }
        (_, Some(identity)) => identity.to_string(),
        (Some("unjournaled"), _) | (None, _) => {
            "merge performed by an unjournaled path".to_string()
        }
        (Some(kind), _) => kind.to_string(),
    }
}

async fn execution_at_resolution(db: &LocalDb, owner_id: &str) -> Option<ExecutionAtResolution> {
    let id = execution_id_for_owner(db, owner_id).await?;
    let execution_status = db
        .query_opt(
            "SELECT status FROM executions WHERE id = ?1",
            (id.clone(),),
            |row| row.opt_text(0),
        )
        .await
        .ok()
        .flatten()
        .flatten();
    let jobs = db
        .query_all(
            "SELECT uri_segment, status FROM jobs WHERE execution_id = ?1 ORDER BY created_at, id",
            (id.clone(),),
            |row| {
                Ok(JobAtResolution {
                    node: row.opt_text(0)?,
                    status: row.opt_text(1)?,
                })
            },
        )
        .await
        .unwrap_or_default();
    Some(ExecutionAtResolution {
        id: Some(id),
        execution_status,
        jobs,
    })
}

async fn lane_snapshot_for_job(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    mr_id: &str,
) -> LaneSnapshot {
    let head = db
        .query_opt(
            "SELECT head_sha FROM merge_requests WHERE id = ?1",
            (mr_id.to_string(),),
            |row| row.opt_text(0),
        )
        .await
        .ok()
        .flatten()
        .flatten();
    // The lanes are read at the head the pull request was judged on, not at
    // the branch's current position: a landing may already have moved the
    // branch onto the commit it landed, whose change against the target is
    // empty and whose tree no lane judged.
    let statuses = match head.as_deref() {
        Some(head) => {
            crate::execution::checks_status::node_check_statuses_at_head(orch, job_id, head).await
        }
        None => crate::execution::checks_status::node_check_statuses(orch, job_id).await,
    }
    .unwrap_or_default();
    let mut systematic_checks = Vec::with_capacity(statuses.len());
    for status in &statuses {
        let Some(mut lane) = LaneAtResolution::from_status(status) else {
            continue;
        };
        lane.observation =
            Some(lane_observation(db, status.observation_id.as_deref(), &lane.state).await);
        systematic_checks.push(lane);
    }
    LaneSnapshot {
        execution: execution_at_resolution(db, job_id).await,
        head,
        systematic_checks,
    }
}

/// The review wave in flight for `job_id` at `head`, which resolving the issue
/// will cancel: its attempt id. A wave still planning has published no head,
/// and its attempt row names it.
async fn review_wave_in_flight(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    head: Option<&str>,
) -> Option<String> {
    if !orch.turn_end_checks_in_flight(job_id) {
        return None;
    }
    match orch.turn_end_check_runtime_status(job_id) {
        Some(wave) => (Some(wave.plan.head.as_str()) == head).then_some(wave.attempt_id),
        None => crate::execution::checks_turn_end::current_turn_end_attempt(db, job_id)
            .await
            .ok()
            .flatten()
            .map(|attempt| attempt.id),
    }
}

/// Name `attempt` on every review lane still waiting on it: the resolution's
/// cascade cancels the wave, so the lane will never have a verdict at this
/// head, and saying only that it had none yet would hide why.
fn record_cancelled_wave(snapshot: &mut LaneSnapshot, attempt: &str) {
    for lane in &mut snapshot.systematic_checks {
        if lane.when.as_deref() == Some("review") && !lane.has_verdict() {
            lane.cancelled_by = Some(attempt.to_string());
            lane.observation = Some(LaneObservation::Absent {
                reason: CANCELLED_BY_RESOLUTION.to_string(),
            });
        }
    }
}

pub async fn latest_resolution_attribution(
    db: &LocalDb,
    mr_id: &str,
) -> Option<StoredPrResolutionAttribution> {
    let mr_id = mr_id.to_string();
    // A Cairn merge can land on GitHub and be observed by a refresh before its
    // own resolution journals, inside the same second. The principal that acted
    // outranks the observation of its act.
    let mut attribution = db.query_opt("SELECT action, actor_kind, actor_identity, surface, created_at, lane_snapshot FROM pr_resolution_attributions WHERE merge_request_id = ?1 ORDER BY created_at DESC, CASE WHEN actor_kind = 'unjournaled' THEN 1 ELSE 0 END LIMIT 1", (mr_id,), |row| Ok(StoredPrResolutionAttribution { action: row.text(0)?, actor_kind: row.text(1)?, actor_identity: row.opt_text(2)?, surface: row.text(3)?, created_at: row.i64(4)?, lane_snapshot: LaneSnapshot::parse(&row.text(5)?) })).await.ok().flatten()?;
    hydrate_lane_failures(db, &mut attribution.lane_snapshot).await;
    Some(attribution)
}

/// Move a `merge_requests` row to its terminal status, returning whether this
/// call moved it.
///
/// The conditional update is the claim on the resolution. A refresh that saw
/// GitHub's merge and Cairn's own merge path can both reach one row, as can two
/// readers that shared one refresh. The claim controls once-only effects;
/// matching terminal retries still run the idempotent settlement cascade. A closed row may still become merged, because GitHub can
/// merge a pull request that was closed and reopened. Nothing moves a merged row.
#[cfg(test)]
async fn claim_merge_request_resolution(
    db: &LocalDb,
    mr_id: &str,
    issue_id: Option<&str>,
    resolution: PrNodeResolution,
    landed: Landed<'_>,
    now: i64,
) -> Result<bool, String> {
    claim_merge_request_resolution_in_generation(db, mr_id, issue_id, resolution, landed, now, None)
        .await
}

#[allow(clippy::too_many_arguments)]
async fn claim_merge_request_resolution_in_generation(
    db: &LocalDb,
    mr_id: &str,
    issue_id: Option<&str>,
    resolution: PrNodeResolution,
    landed: Landed<'_>,
    now: i64,
    expected_generation: Option<i64>,
) -> Result<bool, String> {
    // The resolution's own time is when it happened; `updated_at` is when the
    // row learned of it.
    let resolved_at = landed.resolved_at.unwrap_or(now);
    let moved = match resolution {
        PrNodeResolution::Merge => {
            db.execute(
                "UPDATE merge_requests
                 SET status = 'merged', merged_at = ?1, updated_at = ?5,
                     merged_commit = COALESCE(?2, merged_commit),
                     merged_source_commit = COALESCE(?3, merged_source_commit),
                     issue_resolution_generation = (
                         SELECT resolution_generation FROM issues WHERE id = ?6
                     )
                 WHERE id = ?4 AND status IN ('open', 'closed')
                   AND (?7 IS NULL OR (SELECT resolution_generation FROM issues WHERE id = ?6) = ?7)",
                params![
                    resolved_at,
                    landed.commit,
                    landed.source,
                    mr_id,
                    now,
                    issue_id,
                    expected_generation
                ],
            )
            .await
        }
        PrNodeResolution::Close => {
            db.execute(
                "UPDATE merge_requests
                 SET status = 'closed', closed_at = ?1, updated_at = ?3,
                     issue_resolution_generation = (
                         SELECT resolution_generation FROM issues WHERE id = ?4
                     )
                 WHERE id = ?2 AND status = 'open'",
                params![resolved_at, mr_id, now, issue_id],
            )
            .await
        }
    }
    .map_err(|e| db_error("Failed to update merge request", e))?;
    if moved > 0 {
        cairn_db::admission_hint::evidence_changed();
    }
    Ok(moved > 0)
}

/// Whether an observed resolution of `mr_id` speaks for its issue: the issue is
/// still in the generation pinned by the terminal claim, and no other pull
/// request of the issue is open or newer. Legacy unpinned claims speak only for
/// generation zero; they cannot acquire authority over explicitly reopened work.
///
/// A person who closed the issue keeps that decision, and a pull request that a
/// newer one superseded does not close the issue the newer one serves.
async fn observation_resolves_issue(
    db: &LocalDb,
    issue_id: &str,
    mr_id: &str,
) -> Result<bool, String> {
    let Some(status) = db
        .query_opt_text(
            "SELECT i.status FROM issues i
              WHERE i.id = ?1
                AND i.resolution_generation = (
                    SELECT COALESCE(issue_resolution_generation, 0)
                      FROM merge_requests WHERE id = ?2
                )
                AND NOT EXISTS (
                    SELECT 1 FROM merge_requests other
                     WHERE other.issue_id = i.id AND other.id <> ?2
                       AND (other.status = 'open'
                            OR other.opened_at > (SELECT opened_at FROM merge_requests WHERE id = ?2))
                )",
            params![issue_id, mr_id],
        )
        .await
        .map_err(|e| db_error("Failed to load the issue an observed resolution belongs to", e))?
    else {
        return Ok(false);
    };
    Ok(status
        .parse::<crate::models::IssueStatus>()
        .is_ok_and(|status| {
            !status.is_terminal()
                || matches!(
                    status,
                    crate::models::IssueStatus::Merged | crate::models::IssueStatus::Closed
                )
        }))
}

/// Resolve the PR's issue through the one terminal cascade.
///
/// The PR has already been merged or closed for real by the time this runs — on
/// GitHub, or as a fold landed in the shared store — so the resolution cannot be
/// refused back out; [`StopFailure::Escalates`] says so. Everything else is the
/// same cascade the status-patch path runs: the issue's live work is stopped
/// through the canonical node stop before the rows move, so no session is closed
/// over a running turn and no batch outlives the issue that owns it.
async fn resolve_issue_for_pr(
    orch: &Orchestrator,
    db: &LocalDb,
    issue_id: &str,
    resolution: Resolution,
    expected_generation: Option<i64>,
    guard: &crate::issues::crud::IssueResolutionGuard,
) -> Result<SettlementReport, String> {
    let status = db
        .query_opt_text(
            "SELECT status FROM issues WHERE id = ?1",
            (issue_id.to_string(),),
        )
        .await
        .map_err(|e| db_error("Failed to read issue resolution", e))?;
    if let Some(status) = status {
        let parsed = status.parse::<crate::models::IssueStatus>();
        let matches = match resolution {
            Resolution::Merged => status == "merged",
            Resolution::Closed => status == "closed",
        };
        if parsed.is_ok_and(|status| status.is_terminal()) && !matches {
            return Ok(SettlementReport::new(resolution));
        }
    }
    let response_run = super::merge_response_run();
    crate::issues::status::resolve_terminal_locked(
        orch,
        db,
        issue_id,
        resolution,
        crate::issues::status::StopFailure::Escalates,
        response_run.as_deref(),
        expected_generation,
        guard,
    )
    .await
    .map_err(|refusal| refusal.to_string())
}

/// Who brought a pull request to its terminal state.
enum Resolver {
    /// A Cairn action that performed the merge or close. `None` is a boundary
    /// that did not say whom it acted for, which is journaled loudly.
    Performed(Option<PrResolutionAttribution>),
    /// A terminal local row, not a new action or a GitHub observation.
    Recorded,
    /// A refresh that saw GitHub report a merge or close Cairn did not perform,
    /// with the title and body GitHub showed when it did.
    Observed(Option<GitHubProse>),
}

/// A pull request's title and body as one GitHub fetch saw them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GitHubProse {
    pub(super) title: String,
    pub(super) body: Option<String>,
}

/// What a merge put where: the commit it wrote to the target, and the commit
/// the source branch is expected to hold once the landing is recorded.
///
/// `source` is an exact identity the landing route accepted, never derived by
/// comparison: the folded or published commit when the landing moved the
/// source bookmark onto it, otherwise the source head the route merged. Branch
/// cleanup deletes a merged branch only while it still holds this commit, so a
/// commit its owner made after the landing is never deleted. `None` when the
/// route cannot name it; cleanup then falls back to a real ancestry proof.
///
/// `resolved_at` is when the merge or close actually happened, when that is
/// known and is not now: GitHub's `merged_at`/`closed_at` for a resolution
/// Cairn only observed. `None` stamps the claim with the time it runs.
#[derive(Debug, Clone, Copy, Default)]
pub struct Landed<'a> {
    pub commit: Option<&'a str>,
    pub source: Option<&'a str>,
    pub resolved_at: Option<i64>,
}

/// Resolve a `pr` node by its owner id (`merge_requests.job_id`): the producing
/// `pr` action_run (CAIRN-1220) or a legacy `create_pr` producing job.
///
/// `merged_commit` is the commit a merge wrote to the target branch, when the
/// caller knows it.
pub async fn resolve_pr_node(
    orch: &Orchestrator,
    owner_id: &str,
    resolution: PrNodeResolution,
    attribution: Option<PrResolutionAttribution>,
    merged_commit: Option<&str>,
) -> Result<(), String> {
    resolve_pr_node_as(
        orch,
        owner_id,
        resolution,
        Landed {
            commit: merged_commit,
            source: None,
            resolved_at: None,
        },
        Resolver::Performed(attribution),
    )
    .await
    .map(|_| ())
}

/// [`resolve_pr_node`] for a merge whose route knows exactly what it landed,
/// including the source commit branch cleanup may later rely on.
#[cfg(test)]
pub(crate) async fn resolve_pr_node_landed(
    orch: &Orchestrator,
    owner_id: &str,
    attribution: Option<PrResolutionAttribution>,
    landed: Landed<'_>,
) -> Result<SettlementReport, String> {
    let result = async {
        let db = crate::execution::routing::routing_db_for_id(&orch.db, owner_id)
            .await
            .map_err(|error| error.to_string())?;
        let context = resolve_merge_mr_context_for_job(&db, owner_id).await?;
        let pin = capture_landing_resolution(&db, &context).await?;
        resolve_pinned_landing(orch, owner_id, pin, attribution, landed).await
    }
    .await;
    match result {
        Ok(report) => Ok(report),
        Err(error) => {
            Ok(
                crate::issues::status::pending_for_job(orch, owner_id, Resolution::Merged, error)
                    .await,
            )
        }
    }
}

/// Resolve a pull request that GitHub reports merged or closed by someone other
/// than Cairn: the operator in GitHub's web UI, an integration agent using the
/// GitHub CLI, a teammate.
///
/// It is the resolution a Cairn merge or close runs. The row moves, the `pr`
/// node's terminal cascade runs, and the observation is journaled once as the
/// resolution's provenance. It differs in one respect: a Cairn action resolves
/// the issue because it acted on the issue's behalf, while an observation only
/// knows the pull request moved, so it resolves the issue only when
/// [`observation_resolves_issue`] says the pull request speaks for it.
///
/// Returns whether this call resolved the row; `false` means it already was.
pub(super) async fn resolve_observed_pr_node(
    orch: &Orchestrator,
    owner_id: &str,
    resolution: PrNodeResolution,
    landed: Landed<'_>,
    github_prose: Option<GitHubProse>,
) -> Result<bool, String> {
    resolve_pr_node_as(
        orch,
        owner_id,
        resolution,
        landed,
        Resolver::Observed(github_prose),
    )
    .await
    .map(|(claimed, _)| claimed)
}

/// Recover one immutable publication, never whichever newer PR the owner may
/// now produce, and never claim a terminal edge for a reopened issue lifetime.
pub(super) async fn resolve_recovered_landing(
    orch: &Orchestrator,
    owner_id: &str,
    mr_id: &str,
    generation: Option<i64>,
    attribution: PrResolutionAttribution,
    landed: Landed<'_>,
) -> Result<SettlementReport, String> {
    let db = crate::execution::routing::routing_db_for_id(&orch.db, owner_id)
        .await
        .map_err(|error| error.to_string())?;
    let context = resolve_merge_mr_context_for_job(&db, owner_id).await?;
    if context.mr.mr_id != mr_id {
        return Err(
            "publication's merge request is no longer the owner's current request".to_string(),
        );
    }
    let generation = context.issue_id.as_ref().map(|_| generation.unwrap_or(0));
    resolve_pinned_landing(
        orch,
        owner_id,
        LandingResolutionContext {
            context,
            generation,
        },
        Some(attribution),
        landed,
    )
    .await
}

/// Retry a recorded local resolution without claiming a new terminal edge.
pub(super) async fn resolve_recorded_pr_node(
    orch: &Orchestrator,
    owner_id: &str,
    resolution: PrNodeResolution,
) -> Result<(), String> {
    resolve_pr_node_as(
        orch,
        owner_id,
        resolution,
        Landed::default(),
        Resolver::Recorded,
    )
    .await
    .map(|_| ())
}

async fn resolve_pr_node_as(
    orch: &Orchestrator,
    owner_id: &str,
    resolution: PrNodeResolution,
    landed: Landed<'_>,
    resolver: Resolver,
) -> Result<(bool, SettlementReport), String> {
    resolve_pr_node_in_context(orch, owner_id, resolution, landed, resolver, None).await
}

/// The identity accepted before a landing can yield to publication or approval.
#[derive(Clone)]
pub(super) struct LandingResolutionContext {
    context: MergeMrContext,
    generation: Option<i64>,
}

pub(super) async fn capture_landing_resolution(
    db: &LocalDb,
    context: &MergeMrContext,
) -> Result<LandingResolutionContext, String> {
    let generation = match context.issue_id.as_deref() {
        Some(issue_id) => Some(
            db.query_opt_i64(
                "SELECT resolution_generation FROM issues WHERE id = ?1",
                params![issue_id],
            )
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "Landing issue no longer exists".to_string())?,
        ),
        None => None,
    };
    Ok(LandingResolutionContext {
        context: context.clone(),
        generation,
    })
}

pub(super) async fn resolve_pinned_landing(
    orch: &Orchestrator,
    owner_id: &str,
    pin: LandingResolutionContext,
    attribution: Option<PrResolutionAttribution>,
    landed: Landed<'_>,
) -> Result<SettlementReport, String> {
    let result = resolve_pr_node_in_context(
        orch,
        owner_id,
        PrNodeResolution::Merge,
        landed,
        Resolver::Performed(attribution),
        Some(pin),
    )
    .await
    .map(|(_, report)| report);
    match result {
        Ok(report) => Ok(report),
        Err(error) => {
            Ok(
                crate::issues::status::pending_for_job(orch, owner_id, Resolution::Merged, error)
                    .await,
            )
        }
    }
}

async fn resolve_pr_node_in_context(
    orch: &Orchestrator,
    owner_id: &str,
    resolution: PrNodeResolution,
    landed: Landed<'_>,
    resolver: Resolver,
    pin: Option<LandingResolutionContext>,
) -> Result<(bool, SettlementReport), String> {
    let merged_commit = landed.commit;
    // Route to the database that owns this PR's producing job (team replica or
    // private DB). Every row this resolution reads or writes — the merge_requests
    // status transition, the issue resolution, the pr action_run completion, and
    // the producing-execution advance — lives in that database. This runs AFTER
    // the merge/close already hit GitHub or the local jj fold, so reading the
    // private DB for a team job would error with “No merge request found” and
    // strand the team replica's PR (and its blocked action run) unresolved.
    let db = crate::execution::routing::routing_db_for_id(&orch.db, owner_id)
        .await
        .map_err(|e| e.to_string())?;
    let expected_generation = pin.as_ref().and_then(|pin| pin.generation);
    let merge_context = match pin.as_ref() {
        Some(pin) => pin.context.clone(),
        None => resolve_merge_mr_context_for_job(&db, owner_id).await?,
    };
    let mr_id = merge_context.mr.mr_id.clone();
    let issue_guard = match merge_context.issue_id.as_deref() {
        Some(issue_id) => Some(crate::issues::crud::lock_resolution(&db, issue_id).await),
        None => None,
    };
    let expected_generation = match (merge_context.issue_id.as_deref(), expected_generation) {
        (Some(_), None) => db
            .query_opt_i64(
                "SELECT CASE WHEN mr.status = 'open' AND ?2 = 0
                             THEN i.resolution_generation
                             ELSE COALESCE(mr.issue_resolution_generation, 0) END
                   FROM merge_requests mr LEFT JOIN issues i ON i.id = mr.issue_id
                  WHERE mr.id = ?1",
                params![
                    mr_id.as_str(),
                    i64::from(matches!(resolver, Resolver::Recorded))
                ],
            )
            .await
            .map_err(|error| error.to_string())?,
        (_, generation) => generation,
    };
    if let (Some(issue_id), Some(expected)) =
        (merge_context.issue_id.as_deref(), expected_generation)
    {
        let current = db
            .query_opt_i64(
                "SELECT resolution_generation FROM issues WHERE id = ?1",
                params![issue_id],
            )
            .await
            .map_err(|error| error.to_string())?;
        if current != Some(expected) {
            return Ok((
                false,
                SettlementReport::new(match resolution {
                    PrNodeResolution::Merge => Resolution::Merged,
                    PrNodeResolution::Close => Resolution::Closed,
                }),
            ));
        }
    }
    let now = chrono::Utc::now().timestamp();
    // Both read before the claim and the cascade it licenses, which cancels
    // the node's review wave: the record is of the lanes as the resolution
    // found them.
    let mut lane_snapshot = lane_snapshot_for_job(orch, &db, owner_id, &mr_id).await;
    let wave_in_flight =
        review_wave_in_flight(orch, &db, owner_id, lane_snapshot.head.as_deref()).await;
    let performed = matches!(resolver, Resolver::Performed(_));
    let claimed = if matches!(resolver, Resolver::Recorded) {
        false
    } else {
        claim_merge_request_resolution_in_generation(
            &db,
            &mr_id,
            merge_context.issue_id.as_deref(),
            resolution,
            landed,
            now,
            expected_generation,
        )
        .await?
    };
    let issue_resolution = match resolution {
        PrNodeResolution::Merge => Resolution::Merged,
        PrNodeResolution::Close => Resolution::Closed,
    };
    let mut report = SettlementReport::new(issue_resolution);
    let expected_status = match resolution {
        PrNodeResolution::Merge => "merged",
        PrNodeResolution::Close => "closed",
    };
    let stored_status = db
        .query_opt_text(
            "SELECT status FROM merge_requests WHERE id = ?1",
            (mr_id.clone(),),
        )
        .await
        .map_err(|e| db_error("Failed to read PR resolution", e))?;
    if stored_status.as_deref() != Some(expected_status) {
        // A late close must not fire the close port of an already merged PR.
        return Ok((false, report));
    }
    if pin.is_some() {
        let current = db
            .query_opt_i64(
                "SELECT 1 FROM merge_requests mr
             WHERE mr.id = ?1 AND mr.job_id = ?2
               AND NOT EXISTS (SELECT 1 FROM merge_requests other
                   WHERE other.job_id = ?2 AND other.id <> mr.id AND other.status = 'open')
               AND (?3 IS NULL OR EXISTS (SELECT 1 FROM issues i
                   WHERE i.id = mr.issue_id AND i.resolution_generation = ?3
                     AND COALESCE(mr.issue_resolution_generation, 0) = ?3))",
                params![
                    mr_id.as_str(),
                    merge_context.mr.job_id.as_str(),
                    expected_generation
                ],
            )
            .await
            .map_err(|error| error.to_string())?;
        if current.is_none() {
            return Ok((claimed, report));
        }
    }
    let issue_to_resolve = match merge_context.issue_id.as_deref() {
        Some(issue_id) if performed && claimed => Some(issue_id),
        Some(issue_id) => observation_resolves_issue(&db, issue_id, &mr_id)
            .await?
            .then_some(issue_id),
        _ => None,
    };
    if let (Some(_), Some(attempt)) = (issue_to_resolve, wave_in_flight.as_deref()) {
        record_cancelled_wave(&mut lane_snapshot, attempt);
    }
    let lane_snapshot = serde_json::to_string(&lane_snapshot)
        .map_err(|e| format!("Failed to record the lane snapshot: {e}"))?;
    let github_prose = match &resolver {
        Resolver::Observed(prose) => prose.clone(),
        Resolver::Performed(_) | Resolver::Recorded => None,
    };
    let attribution = match resolver {
        Resolver::Recorded => Some(PrResolutionAttribution {
            actor_kind: "unjournaled",
            actor_identity: None,
            surface: "recorded-resolution-retry",
        }),
        Resolver::Performed(attribution) => Some(attribution.unwrap_or_else(|| {
            log::error!(
                "merge performed by an unjournaled path: owner={owner_id} resolution={resolution:?}"
            );
            PrResolutionAttribution {
                actor_kind: "unjournaled",
                actor_identity: None,
                surface: "unjournaled-path",
            }
        })),
        // Only the call that resolved the row journals the observation, so a
        // resolution observed by many readers is recorded once.
        Resolver::Observed(_) => {
            let recorded = db.query_opt_text(
                "SELECT id FROM pr_resolution_attributions WHERE merge_request_id = ?1 AND action = ?2 LIMIT 1",
                params![mr_id.as_str(), if matches!(resolution, PrNodeResolution::Merge) { "merge" } else { "close" }],
            ).await.map_err(|e| db_error("Failed to read PR attribution", e))?;
            recorded
                .is_none()
                .then(PrResolutionAttribution::github_observation)
        }
    };
    if let Some(attribution) = attribution {
        db.execute(
            "INSERT INTO pr_resolution_attributions (id, merge_request_id, action, actor_kind, actor_identity, surface, lane_snapshot, created_at) SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8
             WHERE NOT EXISTS (
                 SELECT 1 FROM pr_resolution_attributions
                 WHERE merge_request_id = ?2 AND action = ?3
                   AND (actor_kind <> 'unjournaled' OR surface <> 'github-refresh-observation'
                        OR ?6 = 'github-refresh-observation')
             )",
            params![uuid::Uuid::new_v4().to_string(), mr_id.as_str(), if matches!(resolution, PrNodeResolution::Merge) { "merge" } else { "close" }, attribution.actor_kind, attribution.actor_identity.as_deref(), attribution.surface, lane_snapshot.as_str(), now],
        ).await.map_err(|e| db_error("Failed to journal PR resolution attribution", e))?;
    }

    if !claimed && matches!(resolution, PrNodeResolution::Merge) {
        db.execute(
            "UPDATE merge_requests
             SET merged_commit = COALESCE(merged_commit, ?1),
                 merged_source_commit = COALESCE(merged_source_commit, ?2)
             WHERE id = ?3 AND status = 'merged'",
            params![merged_commit, landed.source, mr_id.as_str()],
        )
        .await
        .map_err(|e| db_error("Failed to record the merged commit", e))?;
    }

    // Only the call that won the claim restores the record, so it is restored
    // once however many readers saw the resolution.
    if claimed {
        restore_sealed_pr_record(
            orch,
            &db,
            &merge_context.mr,
            landed.resolved_at.unwrap_or(now),
            github_prose.as_ref(),
        )
        .await;
    }

    // Resolving the PR resolves its issue, and resolving an issue runs the
    // terminal cascade: the issue's live work is stopped, its queued work is
    // cancelled, its sessions close, and any in-flight turn-end review suite is
    // quit (CAIRN-2648, CAIRN-3253).
    if let Some(issue_id) = issue_to_resolve {
        report = match resolve_issue_for_pr(
            orch,
            &db,
            issue_id,
            issue_resolution,
            expected_generation,
            issue_guard.as_ref().expect("issue resolution hold"),
        )
        .await
        {
            Ok(report) => report,
            Err(error) => {
                crate::issues::status::pending_for_job(orch, owner_id, issue_resolution, error)
                    .await
            }
        };
    }

    let finish: Result<(), String> = async {
    if let Some(issue_id) = issue_to_resolve {
        if merge_context.has_triage_batch {
            let result = match resolution {
                PrNodeResolution::Merge => {
                    crate::memories::db::resolve_triage_batch_on_merge(&db, issue_id).await
                }
                PrNodeResolution::Close => {
                    crate::memories::db::revert_triage_batch_on_close(&db, issue_id).await
                }
            };
            match result {
                Ok(ids) if !ids.is_empty() => log::info!(
                    "Resolved {} canon memory triage row(s) for issue {} via {:?}",
                    ids.len(),
                    issue_id,
                    resolution
                ),
                Ok(_) => {}
                Err(error) => log::warn!(
                    "Memory triage {:?} reconciliation failed for issue {}: {}",
                    resolution,
                    issue_id,
                    error
                ),
            }
        }

        if matches!(resolution, PrNodeResolution::Close) {
            match crate::memories::db::discard_draft_memories_for_closed_issue(&db, issue_id).await
            {
                Ok(ids) if !ids.is_empty() => log::info!(
                    "Discarded {} draft memory row(s) for closed issue {}",
                    ids.len(),
                    issue_id
                ),
                Ok(_) => {}
                Err(error) => log::warn!(
                    "Failed to discard draft memories for closed issue {}: {}",
                    issue_id,
                    error
                ),
            }
        }
    }

    let port = match resolution {
        PrNodeResolution::Merge => "merge",
        PrNodeResolution::Close => "close",
    };
    crate::pr_data::ports::fire_pr_node_port_for_owner(&db, owner_id, port).await?;
    // A first-class `pr` action_run was `Blocked` while the PR was open;
    // resolution completes it so the execution can finish. The durable MR owner
    // is the producing job, so the action_run is found by parent_job_id. Legacy
    // `create_pr` PRs have no action_run to flip; a single-job recompute covers
    // that path. See CAIRN-1220.
    match complete_pr_action_run_if_owner(&db, owner_id, now).await? {
        Some(execution_id) => {
            crate::execution::advancement::recompute_execution_jobs(orch, &execution_id)?
        }
        None => crate::execution::advancement::recompute_job(orch, owner_id)?,
    }
    if let Err(e) = advance_producing_execution_after_pr_resolution(orch, &mr_id).await {
        log::warn!(
            "Failed to advance producing execution after PR resolution: {}",
            e
        );
    }

    if claimed && matches!(resolution, PrNodeResolution::Merge) {
        if let Some(issue_id) = issue_to_resolve.map(str::to_string) {
            let orch_for_memories = orch.clone();
            cairn_common::task::spawn(async move {
                let started = std::time::Instant::now();
                match crate::memories::commands::confirm_and_spawn_drafts_for_merged_issue(
                    orch_for_memories,
                    &issue_id,
                )
                .await
                {
                    Ok(spawned) if !spawned.is_empty() => log::info!(
                        "Confirmed draft memories for merged issue {} and spawned {} triage issue(s) in {:?}",
                        issue_id,
                        spawned.len(),
                        started.elapsed()
                    ),
                    Ok(_) => log::info!(
                        "Confirmed draft memories for merged issue {} with no triage issue spawned in {:?}",
                        issue_id,
                        started.elapsed()
                    ),
                    Err(error) => log::warn!(
                        "Failed to confirm draft memories for merged issue {}: {}",
                        issue_id,
                        error
                    ),
                }
            });
        }

        // The base-advance follow-up (importing origin's default, base checks,
        // service and code-map notices, re-pointing children of the merged
        // branch) touches no sibling branch and has no bearing on whether THIS
        // PR is merged. It is best-effort and idempotent, so spawn it off the
        // synchronous merge path: the resolution — and the "merging" button —
        // returns immediately, and the reconcile lands in the background.
        // Recorded as the merge attempt's own `merge_downstream` record, so any
        // branch work or wake it causes is attributed to this merge.
        let orch = orch.clone();
        let owner_id = owner_id.to_string();
        cairn_common::task::spawn(super::merge_metrics::downstream(async move {
            let started = std::time::Instant::now();
            let notified = crate::orchestrator::base_advance::notify_downstream_of_base_advance(
                &orch, &owner_id,
            )
            .await;
            if let Err(e) = &notified {
                log::warn!("Failed to notify downstream jobs of base advance: {}", e);
            }
            log::info!(
                "Deferred downstream base-advance reconcile for owner {} took {:?}",
                owner_id,
                started.elapsed()
            );
            notified
        }));
    }

    if claimed {
        announce_pr_resolution(orch, &merge_context);
    }
    Ok(())
    }.await;
    if let Err(error) = finish {
        report.resources.extend(
            crate::issues::status::pending_for_job(orch, owner_id, issue_resolution, error)
                .await
                .resources,
        );
    }
    Ok((claimed, report))
}

/// The chain a create-pr version written after its pull request resolved is
/// kept in once the resolved record is restored over it. Its own name keeps it
/// out of the pull-request artifact's identity, so nothing ever syncs it to
/// GitHub, and labels it wherever the node's artifacts are listed.
pub(crate) const POST_MERGE_DRAFT_CHAIN: &str = "create-pr-post-merge-draft";

const CREATE_PR_CHAIN: &str = "create-pr";

/// One stored version of a create-pr chain.
struct ChainVersion {
    version: i32,
    data: serde_json::Value,
    created_at: i64,
    confirmed: bool,
}

impl ChainVersion {
    fn says(&self, prose: &GitHubProse) -> bool {
        let title = self.data.get("title").and_then(serde_json::Value::as_str);
        let body = self.data.get("body").and_then(serde_json::Value::as_str);
        title.is_some_and(|title| same_prose(title, &prose.title))
            && same_prose(body.unwrap_or(""), prose.body.as_deref().unwrap_or(""))
    }
}

/// GitHub stores a body it was sent with CRLF line ends and trailing space
/// trimmed, so equal prose compares equal only after the same normalization.
fn same_prose(a: &str, b: &str) -> bool {
    let normalize = |text: &str| text.replace("\r\n", "\n").trim().to_string();
    normalize(a) == normalize(b)
}

/// The lines a resolved pull request's live section adds: the record is
/// sealed, and where a draft written after the resolution is kept, if one was.
pub(super) async fn render_sealed_record(
    db: &LocalDb,
    mr_id: &str,
    pr_number: Option<i32>,
    state: &crate::models::PrState,
) -> Option<String> {
    let verb = match state {
        crate::models::PrState::Merged => "merged",
        crate::models::PrState::Closed => "closed",
        crate::models::PrState::Open | crate::models::PrState::Unpublished => return None,
    };
    let subject = pr_number
        .map(|number| format!("PR #{number}"))
        .unwrap_or_else(|| "This change".to_string());
    let mut out = format!("Sealed: {subject} {verb}; this record is what {verb}.\n");
    let job_id = create_pr_artifact_job_for_merge_request(db, mr_id)
        .await
        .ok()
        .flatten()?;
    let has_draft = db
        .query_opt_text(
            "SELECT id FROM artifacts WHERE job_id = ?1 AND output_name = ?2 LIMIT 1",
            params![job_id.as_str(), POST_MERGE_DRAFT_CHAIN],
        )
        .await
        .ok()
        .flatten()
        .is_some();
    if has_draft {
        if let Some(node) = crate::messages::delivery::node_uri_for_job(db, &job_id).await {
            out.push_str(&format!(
                "A draft written after the {} is kept at {node}/{POST_MERGE_DRAFT_CHAIN}\n",
                if verb == "merged" { "merge" } else { "close" }
            ));
        }
    }
    Some(out)
}

async fn load_create_pr_chain(db: &LocalDb, job_id: &str) -> Result<Vec<ChainVersion>, String> {
    let job_id = job_id.to_string();
    let rows = db
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT version, data, created_at, confirmed FROM artifacts
                          WHERE job_id = ?1 AND output_name = ?2
                          ORDER BY version",
                        params![job_id.as_str(), CREATE_PR_CHAIN],
                    )
                    .await?;
                let mut versions = Vec::new();
                while let Some(row) = rows.next().await? {
                    versions.push((
                        row.i64(0)? as i32,
                        row.text(1)?,
                        row.i64(2)?,
                        row.opt_i64(3)?.unwrap_or(0) != 0,
                    ));
                }
                Ok(versions)
            })
        })
        .await
        .map_err(|e| db_error("Failed to load the create-pr artifact's versions", e))?;
    Ok(rows
        .into_iter()
        .filter_map(|(version, data, created_at, confirmed)| {
            Some(ChainVersion {
                version,
                data: serde_json::from_str(&data).ok()?,
                created_at,
                confirmed,
            })
        })
        .collect())
}

/// Put a resolved pull request's create-pr record back to what resolved.
///
/// A create-pr version written after the pull request's real resolution time
/// was written over a sealed record: the resolution had not been seen yet. The
/// version at or before that time is appended as the chain's newest (history
/// stays append-only), and the newest post-resolution version is kept in
/// [`POST_MERGE_DRAFT_CHAIN`] rather than lost. When the record has no version
/// from before the resolution, GitHub's own prose is the record, unless it is
/// one of the post-resolution versions.
///
/// GitHub is edited back only when its text is one of those post-resolution
/// versions, which only Cairn's own sync puts there; prose someone else wrote
/// on GitHub is theirs. A failure is logged, never raised: the resolution has
/// already happened.
async fn restore_sealed_pr_record(
    orch: &Orchestrator,
    db: &LocalDb,
    mr: &MrContext,
    resolved_at: i64,
    github: Option<&GitHubProse>,
) {
    if let Err(error) = try_restore_sealed_pr_record(orch, db, mr, resolved_at, github).await {
        log::warn!(
            "Could not restore the resolved record of merge request {}: {error}",
            mr.mr_id
        );
    }
}

async fn try_restore_sealed_pr_record(
    orch: &Orchestrator,
    db: &LocalDb,
    mr: &MrContext,
    resolved_at: i64,
    github: Option<&GitHubProse>,
) -> Result<(), String> {
    let Some(job_id) = create_pr_artifact_job_for_merge_request(db, &mr.mr_id).await? else {
        return Ok(());
    };
    let (before, after): (Vec<_>, Vec<_>) = load_create_pr_chain(db, &job_id)
        .await?
        .into_iter()
        .partition(|version| version.created_at <= resolved_at);
    let Some(draft) = after.last() else {
        return Ok(());
    };
    let github_is_a_draft = github.is_some_and(|prose| after.iter().any(|v| v.says(prose)));
    let record = match (before.last(), github) {
        (Some(version), _) => version.data.clone(),
        (None, Some(prose)) if !github_is_a_draft => serde_json::json!({
            "title": prose.title,
            "body": prose.body,
        }),
        _ => {
            log::warn!(
                "Merge request {} resolved before its create-pr artifact's first version and \
                 GitHub shows a post-resolution version, so no resolved record is known to restore",
                mr.mr_id
            );
            return Ok(());
        }
    };
    let stored = crate::mcp::handlers::comments_artifacts::append_restored_artifact_versions(
        orch,
        &job_id,
        vec![
            (CREATE_PR_CHAIN.to_string(), record.clone(), draft.confirmed),
            (
                POST_MERGE_DRAFT_CHAIN.to_string(),
                draft.data.clone(),
                draft.confirmed,
            ),
        ],
    )
    .await?;

    let node = crate::messages::delivery::node_uri_for_job(db, &job_id)
        .await
        .unwrap_or_else(|| format!("job {job_id}"));
    let subject = mr
        .github_pr_number
        .map(|number| format!("PR #{number}"))
        .unwrap_or_else(|| format!("merge request {}", mr.mr_id));
    log::info!(
        "Restored {subject}'s resolved record (create-pr v{}); the post-resolution draft v{} is \
         kept at {node}/{POST_MERGE_DRAFT_CHAIN}",
        stored.first().copied().unwrap_or_default(),
        draft.version
    );

    if let (true, Some(pr_number), false, Some(title)) = (
        github_is_a_draft,
        mr.github_pr_number,
        mr.is_local,
        record.get("title").and_then(serde_json::Value::as_str),
    ) {
        let body = record.get("body").and_then(serde_json::Value::as_str);
        if let Err(error) = super::create_pr::restore_pull_request_prose(
            orch,
            db,
            &mr.repo_path,
            &mr.mr_id,
            pr_number,
            title,
            body,
        )
        .await
        {
            log::warn!("Could not put {subject}'s resolved title and body back on GitHub: {error}");
        }
    }
    Ok(())
}

/// Merged/closed is the settled terminal state the PR snapshot caches without
/// an expiry, so every write that moves a resolved row advances the generation
/// and tells the row's readers.
fn announce_pr_resolution(orch: &Orchestrator, merge_context: &MergeMrContext) {
    orch.invalidate_pr_refresh(&merge_context.mr.job_id, "pull-request-resolved");
    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::merge_request_db_change_ids(
            "update",
            Some(&merge_context.mr.mr_id),
            Some(&merge_context.mr.job_id),
            merge_context.issue_id.as_deref(),
            &merge_context.project_id,
        ),
    );
    if let Some(issue_id) = merge_context.issue_id.as_deref() {
        let _ = orch.services.emitter.emit(
            "db-change",
            crate::notify::issue_db_change_ids("update", issue_id, Some(&merge_context.project_id)),
        );
    }
}

/// If `owner_id` is a producing job with a blocked `pr` action_run child, mark
/// that action_run `complete` and return its execution_id. Also accepts an
/// action_run id for unrepaired historical rows. Returns `None` for legacy
/// `create_pr` jobs with no first-class PR action.
async fn complete_pr_action_run_if_owner(
    db: &LocalDb,
    owner_id: &str,
    now: i64,
) -> Result<Option<String>, String> {
    let owner_id = owner_id.to_string();
    db.write(|conn| {
        let owner_id = owner_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, execution_id
                     FROM (
                         SELECT id, execution_id, 0 AS priority
                         FROM action_runs
                         WHERE parent_job_id = ?1
                         UNION ALL
                         SELECT id, execution_id, 1 AS priority
                         FROM action_runs
                         WHERE id = ?1
                     )
                     ORDER BY priority
                     LIMIT 1",
                    params![owner_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let action_run_id = row.text(0)?;
            let execution_id = row.text(1)?;
            drop(rows);
            conn.execute(
                "UPDATE action_runs SET status = 'complete', completed_at = COALESCE(completed_at, ?1) WHERE id = ?2 AND status NOT IN ('complete', 'failed')",
                params![now, action_run_id.as_str()],
            )
            .await?;
            Ok(Some(execution_id))
        })
    })
    .await
    .map_err(|e| db_error("Failed to complete PR action_run", e))
}

/// After a `merge_requests` row transitions to merged or closed, enqueue an
/// `AdvanceDag` for the execution that produced the PR so any downstream node
/// gated on PR-merged-ness wakes. No-op when the row has no linked job or the
/// job has no `execution_id` (manually-attached or non-recipe PRs).
///
/// Idempotency: callers may invoke this more than once for the same `mr_id`
/// without harm. Each call enqueues a fresh outbox entry and sends a follow-on
/// `AdvanceDag`; `reduce_dag` is idempotent, so a duplicate advancement is
/// cheap and strictly safer than dropping one.
///
/// Errors are swallowed at the call sites — webhook handlers must not return
/// errors that GitHub will retry, and the durable outbox row is the recovery
/// mechanism if the in-memory send is lost.
pub async fn advance_producing_execution_after_pr_resolution(
    orch: &Orchestrator,
    mr_id: &str,
) -> Result<(), String> {
    let mr_id_owned = mr_id.to_string();
    // Route to the owning database (team replica or private DB); the PR's
    // producing job/action_run and execution live there. Best-effort: a routing
    // failure logs and no-ops, matching the swallow-errors contract below.
    let db = match crate::execution::routing::routing_db_for_id(&orch.db, mr_id).await {
        Ok(db) => db,
        Err(e) => {
            log::warn!(
                "Failed to route producing-execution lookup for mr {}: {}",
                mr_id,
                e
            );
            return Ok(());
        }
    };
    let execution_id = match db
        .query_opt_text(
            // The PR's owner (`merge_requests.job_id`) is either a producing
            // job (legacy `create_pr`) or a producing `pr` action_run
            // (CAIRN-1220); resolve the execution from whichever it is.
            "SELECT COALESCE(j.execution_id, ar.execution_id)
             FROM merge_requests mr
             LEFT JOIN jobs j ON mr.job_id = j.id
             LEFT JOIN action_runs ar ON mr.job_id = ar.id
             WHERE mr.id = ?1
             LIMIT 1",
            params![mr_id_owned.as_str()],
        )
        .await
    {
        Ok(value) => value,
        Err(e) => {
            log::warn!(
                "Failed to look up producing execution for mr {}: {}",
                mr_id,
                e
            );
            return Ok(());
        }
    };

    let Some(execution_id) = execution_id else {
        log::debug!(
            "No producing execution for mr {} — skipping DAG advance",
            mr_id
        );
        return Ok(());
    };

    if let Err(error) = crate::workflow_host::runtime(orch)
        .enqueue_advance(&execution_id)
        .await
    {
        log::warn!(
            "Failed to enqueue advance_dag outbox entry for execution {execution_id}: {error}"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pr_data::actions::test_support::{
        migrated_db, seed_pr_node_merge_request_for_artifact_job, test_orchestrator,
    };
    use crate::services::testing::MockGitClient;

    /// Another job on the PR node fixture's issue. `recipe_node_id` stays NULL so
    /// it is extra work on the issue rather than a second claimant of a snapshot
    /// node.
    async fn seed_issue_job(db: &LocalDb, job_id: &str, node_name: &str, status: &str) {
        let job_id = job_id.to_string();
        let node_name = node_name.to_string();
        let status = status.to_string();
        db.execute(
            "INSERT INTO jobs (id, execution_id, node_name, status, issue_id, project_id, created_at, updated_at)
             VALUES (?1, 'exec-pr-node', ?2, ?3, 'issue-pr-node', 'proj-pr-node', 2, 2)",
            params![job_id.as_str(), node_name.as_str(), status.as_str()],
        )
        .await
        .unwrap();
    }

    /// Give an existing job the session, run, and running turn a live agent holds
    /// — the shape the canonical stop acts on.
    async fn attach_live_session(db: &LocalDb, job_id: &str) {
        let job_id = job_id.to_string();
        db.write(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let session_id = format!("session-{job_id}");
                let run_id = format!("run-{job_id}");
                let turn_id = format!("turn-{job_id}");
                conn.execute(
                    "INSERT INTO sessions (id, job_id, status, created_at, updated_at)
                     VALUES (?1, ?2, 'open', 1, 1)",
                    params![session_id.as_str(), job_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs (id, project_id, job_id, chat_id, status, session_id, created_at, updated_at, start_mode)
                     VALUES (?1, 'proj-pr-node', ?2, NULL, 'live', ?3, 1, 1, 'resume')",
                    params![run_id.as_str(), job_id.as_str(), session_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, 1, 'running', 1, 1)",
                    params![
                        turn_id.as_str(),
                        session_id.as_str(),
                        run_id.as_str(),
                        job_id.as_str()
                    ],
                )
                .await?;
                conn.execute(
                    "UPDATE jobs SET current_session_id = ?1, current_turn_id = ?2 WHERE id = ?3",
                    params![session_id.as_str(), turn_id.as_str(), job_id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn authenticated_merge_caller_survives_resolution_until_provider_handoff() {
        use crate::agent_process::process::RunHandle;
        use crate::orchestrator::lifecycle;

        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        seed_issue_job(&db, "merge-caller", "caller", "running").await;
        attach_live_session(&db, "merge-caller").await;
        db.execute(
            "UPDATE jobs SET uri_segment='caller' WHERE id='merge-caller'",
            (),
        )
        .await
        .unwrap();
        let caller_uri = crate::jobs::queries::home_uri_for_job(&db, "merge-caller")
            .await
            .unwrap()
            .expect("the caller fixture has a canonical address");
        let orch = test_orchestrator(db, MockGitClient::new());
        orch.process_state.processes.lock().unwrap().register(
            "run-merge-caller".to_string(),
            RunHandle::test_handle(Some("session-merge-caller"), Some("merge-caller")),
        );
        // The authenticated caller is not the PR's owner ("builder-job").
        let report = super::super::merge::preserve_merge_response(
            &orch,
            Some("run-merge-caller"),
            resolve_pr_node_landed(&orch, "builder-job", None, Landed::default()),
        )
        .await
        .unwrap();
        assert_eq!(
            orch.process_state.has_process("run-merge-caller"),
            Some(true)
        );
        assert!(orch.process_state.terminal_tool_armed("run-merge-caller"));
        assert_eq!(
            scalar(
                &orch.db.local,
                "SELECT status FROM jobs WHERE id = ?1",
                "merge-caller"
            )
            .await
            .as_deref(),
            Some("cancelled")
        );
        assert_eq!(
            scalar(
                &orch.db.local,
                "SELECT status FROM sessions WHERE id = ?1",
                "session-merge-caller"
            )
            .await
            .as_deref(),
            Some("closed")
        );
        assert!(
            report
                .resources
                .iter()
                .any(|resource| resource.uri == caller_uri),
            "{report}"
        );

        // A concurrent settlement, without the invoking task's local identity,
        // must not interrupt a response still travelling to its provider.
        lifecycle::stop_job_for_resolution(&orch, "merge-caller", Resolution::Merged).unwrap();
        assert_eq!(
            orch.process_state.has_process("run-merge-caller"),
            Some(true)
        );
        assert!(!lifecycle::release_and_stop_process(
            &orch,
            "run-merge-caller"
        ));
        assert_eq!(
            scalar(
                &orch.db.local,
                "SELECT status FROM runs WHERE id = ?1",
                "run-merge-caller"
            )
            .await
            .as_deref(),
            Some("live")
        );

        // This is the same acknowledged tool-result boundary used by providers.
        assert!(lifecycle::finish_resolution_response(
            &orch,
            "run-merge-caller"
        ));
        assert_ne!(
            orch.process_state.has_process("run-merge-caller"),
            Some(true)
        );
        assert_eq!(
            scalar(
                &orch.db.local,
                "SELECT status || '/' || exit_reason FROM runs WHERE id = ?1",
                "run-merge-caller"
            )
            .await
            .as_deref(),
            Some("exited/issue_merged")
        );
        assert!(!lifecycle::finish_resolution_response(
            &orch,
            "run-merge-caller"
        ));
    }

    async fn scalar(db: &LocalDb, sql: &str, id: &str) -> Option<String> {
        db.query_opt_text(sql, params![id]).await.unwrap()
    }

    async fn live_run_count(db: &LocalDb) -> i64 {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT COUNT(*) FROM runs WHERE status IN ('starting', 'live')",
                        (),
                    )
                    .await?;
                rows.next().await?.expect("count row").i64(0)
            })
        })
        .await
        .unwrap()
    }

    async fn seed_parent_coordinator(db: &LocalDb, muted: bool) {
        db.execute_script(
            "INSERT INTO issues
               (id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES
               ('parent-issue', 'proj-pr-node', 1, 'Parent thread', 'active', 'active', 'none', 1, 1);
             UPDATE issues SET parent_issue_id = 'parent-issue' WHERE id = 'issue-pr-node';
             UPDATE merge_requests SET github_pr_number = 3023 WHERE id = 'mr-pr-node';
             INSERT INTO jobs
               (id, project_id, issue_id, status, current_session_id, created_at, updated_at)
             VALUES
               ('parent-coordinator', 'proj-pr-node', 'parent-issue', 'running', 'parent-session', 2, 2);",
        )
        .await
        .unwrap();
        if muted {
            db.execute(
                "INSERT INTO wake_subscriptions
                   (id, job_id, source_kind, source_ref, fact_kinds_json, state,
                    created_by, created_at, updated_at, one_shot)
                 VALUES
                   ('muted-child', 'parent-coordinator', 'issue', 'cairn://p/proj/3',
                    '[\"resolved\"]', 'muted', 'agent', 2, 2, 0)",
                (),
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn operator_ui_merge_notifies_parent_and_mute_downgrades_to_digest() {
        use crate::orchestrator::attention_delivery::render_push_resolved;
        use crate::orchestrator::attention_push::{list_pending, Wake};

        for muted in [false, true] {
            let db = migrated_db().await;
            seed_pr_node_merge_request_for_artifact_job(&db).await;
            seed_parent_coordinator(&db, muted).await;
            let orch = test_orchestrator(db, MockGitClient::new());

            resolve_pr_node(
                &orch,
                "builder-job",
                PrNodeResolution::Merge,
                Some(PrResolutionAttribution::operator_ui()),
                None,
            )
            .await
            .expect("the operator UI merge resolves the child");

            let pushes = list_pending(&orch.db.local, "parent-coordinator")
                .await
                .unwrap();
            assert_eq!(pushes.len(), 1, "muted={muted}");
            assert_eq!(
                pushes[0].wake,
                if muted { Wake::Passive } else { Wake::Wake },
                "muted={muted}"
            );
            let rendered = render_push_resolved(&orch, &pushes[0]).await;
            assert!(rendered.contains("proj-3 \"Issue\" merged"), "{rendered}");
            assert!(
                rendered.contains("PR #3023, by operator (UI)"),
                "{rendered}"
            );
        }
    }

    /// The close door announces exactly as the merge door does. Neither the
    /// UI close nor the recipe `close_issue` action calls anything after the
    /// cascade, so the announcement has to come from the cascade itself.
    #[tokio::test(flavor = "current_thread")]
    async fn operator_ui_close_notifies_parent() {
        use crate::orchestrator::attention_delivery::render_push_resolved;
        use crate::orchestrator::attention_push::{list_pending, Wake};

        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        seed_parent_coordinator(&db, false).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        resolve_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Close,
            Some(PrResolutionAttribution::operator_ui()),
            None,
        )
        .await
        .expect("the operator UI close resolves the child");

        let pushes = list_pending(&orch.db.local, "parent-coordinator")
            .await
            .unwrap();
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].wake, Wake::Wake);
        let rendered = render_push_resolved(&orch, &pushes[0]).await;
        assert!(rendered.contains("proj-3 \"Issue\" closed"), "{rendered}");
    }

    /// Resolving an issue that is already terminal crosses no edge, so it is
    /// not a second announcement — even though it restamps the resolution
    /// instant the push fingerprint is keyed to, which is exactly what would
    /// let a delivered notice fire again.
    #[tokio::test(flavor = "current_thread")]
    async fn resolving_an_already_resolved_issue_announces_nothing_new() {
        use crate::orchestrator::attention_push::list_pending;

        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        seed_parent_coordinator(&db, false).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        resolve_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Merge,
            Some(PrResolutionAttribution::operator_ui()),
            None,
        )
        .await
        .expect("the merge resolves the child");
        assert_eq!(
            list_pending(&orch.db.local, "parent-coordinator")
                .await
                .unwrap()
                .len(),
            1
        );
        orch.db
            .local
            .execute(
                "UPDATE attention_pushes SET delivered_event_id = 'delivered'
                  WHERE recipient = 'parent-coordinator'",
                (),
            )
            .await
            .unwrap();

        crate::issues::status::resolve_terminal(
            &orch,
            &orch.db.local,
            "issue-pr-node",
            Resolution::Merged,
            crate::issues::status::StopFailure::Escalates,
        )
        .await
        .expect("a repeat resolution lands");

        assert!(
            list_pending(&orch.db.local, "parent-coordinator")
                .await
                .unwrap()
                .is_empty(),
            "the coordinator already heard about this resolution"
        );
    }

    /// The PR merge is the door CAIRN-3241 came through: the PR merged, the issue
    /// row flipped, and the builder went on running test suites against a merged
    /// issue — because this path resolved the issue directly instead of running
    /// the cascade the confirmed-close path already had. It runs the same one now.
    #[tokio::test(flavor = "current_thread")]
    async fn merging_a_pr_stops_the_issues_live_work() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        seed_issue_job(&db, "live-builder", "builder", "running").await;
        attach_live_session(&db, "live-builder").await;
        seed_issue_job(&db, "queued-reviewer", "reviewer", "pending").await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;

        resolve_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Merge,
            Some(PrResolutionAttribution::agent(
                "cairn://p/prj/1/1/thread".to_string(),
            )),
            None,
        )
        .await
        .expect("the merge resolution lands");

        let attribution = latest_resolution_attribution(db, "mr-pr-node")
            .await
            .expect("the resolution journals attribution");
        assert_eq!(attribution.actor_kind, "agent");
        assert_eq!(
            attribution.actor_identity.as_deref(),
            Some("cairn://p/prj/1/1/thread")
        );
        assert_eq!(
            attribution
                .lane_snapshot
                .execution
                .as_ref()
                .and_then(|execution| execution.id.as_deref()),
            Some("exec-pr-node")
        );

        assert_eq!(
            scalar(
                db,
                "SELECT status FROM issues WHERE id = ?1",
                "issue-pr-node"
            )
            .await,
            Some("merged".to_string())
        );

        // The running builder went through terminal resolution: its turn is
        // cancelled with the typed merge cause and its run is not left executing.
        assert_eq!(
            scalar(
                db,
                "SELECT state || '/' || end_reason FROM turns WHERE id = ?1",
                "turn-live-builder"
            )
            .await,
            Some("cancelled/issue_merged".to_string())
        );
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM runs WHERE id = ?1",
                "run-live-builder"
            )
            .await,
            Some("exited".to_string())
        );
        assert_eq!(
            scalar(
                db,
                "SELECT exit_reason FROM runs WHERE id = ?1",
                "run-live-builder"
            )
            .await,
            Some("issue_merged".to_string())
        );

        // Work that never started is cancelled rather than left queued against a
        // merged issue.
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM jobs WHERE id = ?1",
                "queued-reviewer"
            )
            .await,
            Some("cancelled".to_string())
        );

        assert_eq!(
            scalar(db, "SELECT status FROM jobs WHERE id = ?1", "live-builder").await,
            Some("cancelled".to_string()),
            "terminal resolution archives the stopped builder rather than leaving it running"
        );

        // And the session closes over stopped work, not running work.
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM sessions WHERE id = ?1",
                "session-live-builder"
            )
            .await,
            Some("closed".to_string())
        );
        assert_eq!(
            live_run_count(db).await,
            0,
            "no run keeps executing against a merged issue"
        );
    }

    /// The half-dead state must be unrepresentable: a session that refuses to
    /// continue while its own runs keep going.
    ///
    /// The cascade's enumeration reads the `jobs` table, so a job that is not
    /// itself live work slips past it — here one already marked `complete` while
    /// still holding an open session and a running turn, which is also what a
    /// turn started between the enumeration and the close looks like. The
    /// postcondition is what closes that gap.
    #[tokio::test(flavor = "current_thread")]
    async fn a_closed_session_is_never_left_with_a_turn_in_flight() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        // `builder-job` is `complete`, so it is not enumerated as live work.
        attach_live_session(&db, "builder-job").await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;

        resolve_pr_node(&orch, "builder-job", PrNodeResolution::Merge, None, None)
            .await
            .expect("the merge resolution lands");

        let phantom = latest_resolution_attribution(db, "mr-pr-node")
            .await
            .expect("an unattributed boundary is journaled loudly");
        assert_eq!(phantom.action, "merge");
        assert_eq!(phantom.actor_kind, "unjournaled");
        assert_eq!(phantom.surface, "unjournaled-path");

        assert_eq!(
            scalar(
                db,
                "SELECT status FROM sessions WHERE id = ?1",
                "session-builder-job"
            )
            .await,
            Some("closed".to_string()),
            "the resolution closes the session"
        );
        assert_eq!(
            scalar(
                db,
                "SELECT state || '/' || end_reason FROM turns WHERE id = ?1",
                "turn-builder-job"
            )
            .await,
            Some("cancelled/issue_merged".to_string()),
            "and nothing it closed is left with a turn in flight"
        );
        assert_eq!(live_run_count(db).await, 0);
    }

    /// The PR door reaches the projection settlement: once the merge lands, no
    /// job on the issue rests running, pending, idle, or blocked — including
    /// the builder the postcondition stopped, which before settlement re-derived
    /// Pending against the merged issue.
    #[tokio::test(flavor = "current_thread")]
    async fn a_terminal_pr_retries_its_unfinished_cascade() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        seed_issue_job(&db, "queued-reviewer", "reviewer", "pending").await;
        db.execute(
            "UPDATE merge_requests SET status = 'merged', merged_at = 42 WHERE id = 'mr-pr-node'",
            (),
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db, MockGitClient::new());
        for _ in 0..2 {
            let report = resolve_pr_node_landed(
                &orch,
                "builder-job",
                Some(PrResolutionAttribution::operator_ui()),
                Landed {
                    commit: Some("landed"),
                    resolved_at: Some(99),
                    ..Landed::default()
                },
            )
            .await
            .unwrap();
            assert!(!report.has_pending(), "{report}");
        }
        resolve_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Close,
            Some(PrResolutionAttribution::operator_ui()),
            None,
        )
        .await
        .unwrap();
        let db = &orch.db.local;
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM pr_node_port_fires WHERE port = 'close'",
                ()
            )
            .await
            .unwrap(),
            Some(0)
        );
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM issues WHERE id = ?1",
                "issue-pr-node"
            )
            .await
            .as_deref(),
            Some("merged")
        );
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM action_runs WHERE id = ?1",
                "pr-action-run"
            )
            .await
            .as_deref(),
            Some("complete")
        );
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM jobs WHERE id = ?1",
                "queued-reviewer"
            )
            .await
            .as_deref(),
            Some("cancelled")
        );
        assert_eq!(
            db.query_opt_i64(
                "SELECT merged_at FROM merge_requests WHERE id = 'mr-pr-node'",
                ()
            )
            .await
            .unwrap(),
            Some(42)
        );
        assert_eq!(db.query_opt_i64("SELECT COUNT(*) FROM pr_resolution_attributions WHERE merge_request_id = 'mr-pr-node'", ()).await.unwrap(), Some(1));
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM pr_node_port_fires WHERE port = 'merge'",
                ()
            )
            .await
            .unwrap(),
            Some(1)
        );
    }

    /// Pause at the publication boundary: the accepted request is fixed before
    /// the owner can replace it or the issue can acquire a successor lifetime.
    #[tokio::test(flavor = "current_thread")]
    async fn performed_landing_keeps_successors_created_during_publication() {
        for replacement in [false, true] {
            let db = migrated_db().await;
            seed_pr_node_merge_request_for_artifact_job(&db).await;
            let orch = test_orchestrator(db, MockGitClient::new());
            let context = resolve_merge_mr_context_for_job(&orch.db.local, "builder-job")
                .await
                .unwrap();
            let pin = capture_landing_resolution(&orch.db.local, &context)
                .await
                .unwrap();
            let (published, publication) = tokio::sync::oneshot::channel();
            let resolver = orch.clone();
            let settling = cairn_common::task::spawn(async move {
                publication.await.unwrap();
                resolve_pinned_landing(
                    &resolver,
                    "builder-job",
                    pin,
                    None,
                    Landed {
                        commit: Some("actually-landed"),
                        ..Landed::default()
                    },
                )
                .await
                .unwrap()
            });
            if replacement {
                // The old row remains durable but no longer belongs to the
                // owner whose next PR must not inherit this landing.
                seed_issue_job(&orch.db.local, "prior-owner", "prior-owner", "complete").await;
                orch.db
                    .local
                    .execute(
                        "UPDATE merge_requests SET job_id = 'prior-owner' WHERE id = 'mr-pr-node'",
                        (),
                    )
                    .await
                    .unwrap();
                orch.db.local.execute(
                    "INSERT INTO merge_requests(id,job_id,project_id,issue_id,title,source_branch,target_branch,status,opened_at,updated_at)
                     VALUES('replacement-mr','builder-job','proj-pr-node','issue-pr-node','Successor','next','main','open',2,2)", (),
                ).await.unwrap();
            } else {
                // Close/reopen is atomic at the generation increment; no
                // elapsed-time ordering or second-resolution timestamp needed.
                orch.db
                    .local
                    .execute(
                        "UPDATE issues SET status = 'closed' WHERE id = 'issue-pr-node'",
                        (),
                    )
                    .await
                    .unwrap();
                crate::issues::crud::unresolve(
                    &orch.db.local,
                    &*orch.services.clock,
                    "issue-pr-node",
                )
                .await
                .unwrap();
                orch.db
                    .local
                    .execute(
                        "UPDATE issues SET status = 'active' WHERE id = 'issue-pr-node'",
                        (),
                    )
                    .await
                    .unwrap();
            }
            seed_issue_job(
                &orch.db.local,
                "successor-work",
                "successor-work",
                "pending",
            )
            .await;
            published.send(()).unwrap();
            let _report = settling.await.unwrap();
            assert_eq!(
                orch.db
                    .local
                    .query_opt_text("SELECT status FROM jobs WHERE id = 'successor-work'", (),)
                    .await
                    .unwrap()
                    .as_deref(),
                Some("pending")
            );
            assert_ne!(
                orch.db
                    .local
                    .query_opt_text("SELECT status FROM issues WHERE id = 'issue-pr-node'", (),)
                    .await
                    .unwrap()
                    .as_deref(),
                Some("merged")
            );
            assert_eq!(
                orch.db
                    .local
                    .query_opt_text(
                        "SELECT status FROM action_runs WHERE id = 'pr-action-run'",
                        (),
                    )
                    .await
                    .unwrap()
                    .as_deref(),
                Some("blocked")
            );
            assert_eq!(
                orch.db
                    .local
                    .query_opt_i64("SELECT COUNT(*) FROM pr_node_port_fires", (),)
                    .await
                    .unwrap(),
                Some(0)
            );
            if replacement {
                assert_eq!(
                    orch.db
                        .local
                        .query_opt_text(
                            "SELECT status FROM merge_requests WHERE id = 'replacement-mr'",
                            (),
                        )
                        .await
                        .unwrap()
                        .as_deref(),
                    Some("open")
                );
            } else {
                assert_eq!(
                    orch.db
                        .local
                        .query_opt_text(
                            "SELECT status FROM merge_requests WHERE id = 'mr-pr-node'",
                            (),
                        )
                        .await
                        .unwrap()
                        .as_deref(),
                    Some("open")
                );
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_recorded_retry_never_claims_an_open_row() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());
        resolve_recorded_pr_node(&orch, "builder-job", PrNodeResolution::Merge)
            .await
            .unwrap();
        assert_eq!(
            orch.db
                .local
                .query_opt_text(
                    "SELECT status FROM merge_requests WHERE id='mr-pr-node'",
                    (),
                )
                .await
                .unwrap()
                .as_deref(),
            Some("open")
        );
        assert_eq!(
            orch.db
                .local
                .query_opt_i64("SELECT COUNT(*) FROM pr_resolution_attributions", (),)
                .await
                .unwrap(),
            Some(0)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recorded_retries_respect_other_prs_but_a_performed_claim_still_resolves() {
        for performed in [false, true] {
            for other_status in ["open", "closed"] {
                let db = migrated_db().await;
                seed_pr_node_merge_request_for_artifact_job(&db).await;
                seed_issue_job(&db, "queued-new", "queued-new", "pending").await;
                let orch = test_orchestrator(db, MockGitClient::new());
                orch.db.local.execute(
                    "INSERT INTO merge_requests(id,job_id,project_id,issue_id,title,source_branch,target_branch,status,opened_at,updated_at)
                     VALUES('other-mr','other-owner','proj-pr-node','issue-pr-node','Other','other-branch','main',?1,?2,2)",
                    params![other_status, if other_status == "open" { 0 } else { 2 }],
                ).await.unwrap();
                if performed {
                    resolve_pr_node(&orch, "builder-job", PrNodeResolution::Merge, None, None)
                        .await
                        .unwrap();
                    assert_eq!(
                        orch.db
                            .local
                            .query_opt_text(
                                "SELECT status FROM issues WHERE id='issue-pr-node'",
                                (),
                            )
                            .await
                            .unwrap()
                            .as_deref(),
                        Some("merged")
                    );
                } else {
                    assert!(claim_merge_request_resolution(
                        &orch.db.local,
                        "mr-pr-node",
                        Some("issue-pr-node"),
                        PrNodeResolution::Merge,
                        Landed::default(),
                        42,
                    )
                    .await
                    .unwrap());
                    resolve_recorded_pr_node(&orch, "builder-job", PrNodeResolution::Merge)
                        .await
                        .unwrap();
                    assert_eq!(
                        orch.db
                            .local
                            .query_opt_text("SELECT status FROM jobs WHERE id='queued-new'", (),)
                            .await
                            .unwrap()
                            .as_deref(),
                        Some("pending")
                    );
                    assert_ne!(
                        orch.db
                            .local
                            .query_opt_text(
                                "SELECT status FROM issues WHERE id='issue-pr-node'",
                                (),
                            )
                            .await
                            .unwrap()
                            .as_deref(),
                        Some("merged")
                    );
                    assert_eq!(orch.db.local.query_opt_text(
                        "SELECT surface FROM pr_resolution_attributions WHERE merge_request_id='mr-pr-node'", (),
                    ).await.unwrap().as_deref(), Some("recorded-resolution-retry"));
                }
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_new_pr_after_reopen_keeps_interrupted_cascade_recovery() {
        struct FixedClock(i64);
        impl crate::services::Clock for FixedClock {
            fn now(&self) -> i64 {
                self.0
            }

            fn now_u64(&self) -> u64 {
                self.0 as u64
            }
        }

        for observed in [false, true] {
            for resolution in [PrNodeResolution::Merge, PrNodeResolution::Close] {
                let db = migrated_db().await;
                seed_pr_node_merge_request_for_artifact_job(&db).await;
                let orch = test_orchestrator(db, MockGitClient::new());
                resolve_pr_node(&orch, "builder-job", PrNodeResolution::Merge, None, None)
                    .await
                    .unwrap();
                let completed = orch
                    .db
                    .local
                    .query_opt_i64(
                        "SELECT completed_at FROM issues WHERE id='issue-pr-node'",
                        (),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                crate::issues::crud::unresolve(
                    &orch.db.local,
                    &FixedClock(completed),
                    "issue-pr-node",
                )
                .await
                .unwrap();
                seed_issue_job(&orch.db.local, "new-owner", "new-owner", "complete").await;
                seed_issue_job(&orch.db.local, "new-queued", "new-queued", "pending").await;
                orch.db.local.execute(
                    "INSERT INTO merge_requests(id,job_id,project_id,issue_id,title,source_branch,target_branch,status,opened_at,updated_at)
                     VALUES('new-mr','new-owner','proj-pr-node','issue-pr-node','New','new-branch','main','open',2,2)", (),
                ).await.unwrap();
                // Claim only: model a crash before any issue settlement.
                assert!(claim_merge_request_resolution(
                    &orch.db.local,
                    "new-mr",
                    Some("issue-pr-node"),
                    resolution,
                    Landed::default(),
                    completed,
                )
                .await
                .unwrap());
                assert_eq!(orch.db.local.query_opt_i64(
                    "SELECT issue_resolution_generation FROM merge_requests WHERE id='new-mr'", (),
                ).await.unwrap(), Some(1));
                assert_eq!(
                    orch.db
                        .local
                        .query_opt_text("SELECT status FROM jobs WHERE id='new-queued'", (),)
                        .await
                        .unwrap()
                        .as_deref(),
                    Some("pending")
                );
                let (claimed, report) = resolve_pr_node_as(
                    &orch,
                    "new-owner",
                    resolution,
                    Landed::default(),
                    if observed {
                        Resolver::Observed(None)
                    } else {
                        Resolver::Recorded
                    },
                )
                .await
                .unwrap();
                assert!(!claimed);
                assert!(!report.has_pending(), "{report}");
                let expected = match resolution {
                    PrNodeResolution::Merge => "merged",
                    PrNodeResolution::Close => "closed",
                };
                assert_eq!(
                    orch.db
                        .local
                        .query_opt_text("SELECT status FROM issues WHERE id='issue-pr-node'", (),)
                        .await
                        .unwrap()
                        .as_deref(),
                    Some(expected)
                );
                assert_eq!(
                    orch.db
                        .local
                        .query_opt_text("SELECT status FROM jobs WHERE id='new-queued'", (),)
                        .await
                        .unwrap()
                        .as_deref(),
                    Some("cancelled")
                );
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fresh_unpinned_pr_claims_use_the_reopened_generation() {
        for observed in [false, true] {
            for resolution in [PrNodeResolution::Merge, PrNodeResolution::Close] {
                let db = migrated_db().await;
                seed_pr_node_merge_request_for_artifact_job(&db).await;
                let orch = test_orchestrator(db, MockGitClient::new());
                crate::issues::crud::unresolve(
                    &orch.db.local,
                    &*orch.services.clock,
                    "issue-pr-node",
                )
                .await
                .unwrap();
                let (claimed, _) = resolve_pr_node_as(
                    &orch,
                    "builder-job",
                    resolution,
                    Landed::default(),
                    if observed {
                        Resolver::Observed(None)
                    } else {
                        Resolver::Performed(None)
                    },
                )
                .await
                .unwrap();
                assert!(claimed);
                assert_eq!(orch.db.local.query_opt_i64(
                    "SELECT issue_resolution_generation FROM merge_requests WHERE id='mr-pr-node'", (),
                ).await.unwrap(), Some(1));
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_resolution_errors_name_canonical_resources() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let report = crate::issues::status::pending_for_job(
            &orch,
            "builder-job",
            Resolution::Merged,
            "unreadable cleanup".to_string(),
        )
        .await;
        assert_eq!(report.resources.len(), 1);
        assert!(report.resources[0].uri.starts_with("cairn://p/"));
        assert_ne!(report.resources[0].uri, "builder-job");
    }

    /// A landed merge cannot be refused just because its retained provider
    /// handle cannot be inspected. Repairing the handle must let a matching
    /// terminal retry finish the cascade, without another resolution edge.
    #[tokio::test(flavor = "current_thread")]
    async fn landed_merge_reports_unconfirmed_process_and_retry_settles_it() {
        use crate::agent_process::process::RunHandle;

        for observed in [false, true] {
            let db = migrated_db().await;
            seed_pr_node_merge_request_for_artifact_job(&db).await;
            seed_issue_job(&db, "live-builder", "builder", "running").await;
            attach_live_session(&db, "live-builder").await;
            seed_issue_job(&db, "queued-reviewer", "reviewer", "pending").await;
            let orch = test_orchestrator(db, MockGitClient::new());
            let handle = RunHandle::test_handle(Some("session-live-builder"), Some("live-builder"));
            let child = handle.child.clone();
            // Poison only the child lock, not the registry. This injects a
            // process-inspection failure without signalling an OS process or
            // waiting for the graceful-stop timeout.
            let poison = child.clone();
            assert!(std::thread::spawn(move || {
                let _guard = poison.lock().unwrap();
                panic!("injected unavailable child handle");
            })
            .join()
            .is_err());
            orch.process_state
                .processes
                .lock()
                .unwrap()
                .register("run-live-builder".to_string(), handle);

            let landed = Landed {
                commit: Some("landed-commit"),
                resolved_at: Some(42),
                ..Landed::default()
            };
            let resolver = || {
                if observed {
                    Resolver::Observed(None)
                } else {
                    Resolver::Performed(Some(PrResolutionAttribution::operator_ui()))
                }
            };
            let (claimed, report) = resolve_pr_node_as(
                &orch,
                "builder-job",
                PrNodeResolution::Merge,
                landed,
                resolver(),
            )
            .await
            .expect("an already-landed merge is not refused by stop failure");
            assert!(claimed);
            assert!(report.has_pending(), "observed={observed}: {report}");
            assert!(report.to_string().contains("cleanup pending"), "{report}");
            assert!(
                !report.to_string().contains("Nothing was resolved"),
                "{report}"
            );
            assert_eq!(
                orch.process_state.has_process("run-live-builder"),
                Some(true)
            );
            let db = &orch.db.local;
            assert_eq!(
                scalar(
                    db,
                    "SELECT status FROM merge_requests WHERE id = ?1",
                    "mr-pr-node"
                )
                .await
                .as_deref(),
                Some("merged"),
            );
            assert_eq!(
                scalar(
                    db,
                    "SELECT status FROM issues WHERE id = ?1",
                    "issue-pr-node"
                )
                .await
                .as_deref(),
                Some("merged"),
            );
            assert_eq!(
                scalar(
                    db,
                    "SELECT status FROM jobs WHERE id = ?1",
                    "queued-reviewer"
                )
                .await
                .as_deref(),
                Some("cancelled"),
            );

            // The provider has now exited (the fixture has no child). Clearing
            // the inspection fault makes that fact observable to reconciliation.
            child.clear_poison();
            let (claimed, report) = resolve_pr_node_as(
                &orch,
                "builder-job",
                PrNodeResolution::Merge,
                landed,
                resolver(),
            )
            .await
            .expect("matching terminal retry finishes remaining settlement");
            assert!(!claimed, "retry must not claim another merge edge");
            assert!(!report.has_pending(), "observed={observed}: {report}");
            assert_eq!(
                orch.process_state.has_process("run-live-builder"),
                Some(false)
            );
            assert_eq!(live_run_count(db).await, 0);
            assert_eq!(
                scalar(
                    db,
                    "SELECT status FROM sessions WHERE id = ?1",
                    "session-live-builder"
                )
                .await
                .as_deref(),
                Some("closed"),
            );
            assert_eq!(
                db.query_opt_i64(
                    "SELECT merged_at FROM merge_requests WHERE id = 'mr-pr-node'",
                    ()
                )
                .await
                .unwrap(),
                Some(42),
            );
            assert_eq!(
                db.query_opt_i64("SELECT COUNT(*) FROM pr_resolution_attributions WHERE merge_request_id = 'mr-pr-node'", ()).await.unwrap(),
                Some(1),
            );
            assert_eq!(
                db.query_opt_i64(
                    "SELECT COUNT(*) FROM pr_node_port_fires WHERE port = 'merge'",
                    ()
                )
                .await
                .unwrap(),
                Some(1),
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn merging_a_pr_leaves_no_job_unsettled() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        attach_live_session(&db, "builder-job").await;
        seed_issue_job(&db, "queued-reviewer", "reviewer", "pending").await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;

        resolve_pr_node(&orch, "builder-job", PrNodeResolution::Merge, None, None)
            .await
            .expect("the merge resolution lands");

        let unsettled = db
            .query_opt_i64(
                "SELECT COUNT(*) FROM jobs WHERE issue_id = 'issue-pr-node'
                   AND status IN ('running', 'pending', 'idle', 'blocked')",
                (),
            )
            .await
            .unwrap()
            .unwrap_or(0);
        assert_eq!(unsettled, 0, "a merged issue holds no unsettled job");
    }

    async fn observation_count(db: &LocalDb) -> i64 {
        db.query_opt_i64(
            "SELECT COUNT(*) FROM pr_resolution_attributions
              WHERE merge_request_id = 'mr-pr-node' AND actor_kind = 'unjournaled'",
            (),
        )
        .await
        .unwrap()
        .unwrap_or(0)
    }

    /// A merge made on GitHub by someone other than Cairn resolves exactly as a
    /// Cairn merge does: the row, its merge commit, the issue, and the `pr`
    /// node's blocked action run. The observation is journaled once, and seeing
    /// it again changes nothing.
    #[tokio::test(flavor = "current_thread")]
    async fn an_observed_merge_resolves_the_row_and_the_pr_node_once() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;
        let hints = cairn_db::admission_hint::sent_on_this_thread();

        let resolved = resolve_observed_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Merge,
            Landed {
                commit: Some("b77e7657"),
                source: None,
                resolved_at: None,
            },
            None,
        )
        .await
        .expect("the observed merge resolves");
        assert!(resolved, "the first observation resolves the row");
        assert!(cairn_db::admission_hint::sent_on_this_thread() > hints);

        let row = |column: &'static str| {
            let sql = format!("SELECT {column} FROM merge_requests WHERE id = ?1");
            async move { scalar(db, &sql, "mr-pr-node").await }
        };
        assert_eq!(row("status").await.as_deref(), Some("merged"));
        assert_eq!(row("merged_commit").await.as_deref(), Some("b77e7657"));
        let merged_at = row("CAST(merged_at AS TEXT)").await;
        assert!(merged_at.is_some(), "merged_at is recorded");
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM issues WHERE id = ?1",
                "issue-pr-node"
            )
            .await
            .as_deref(),
            Some("merged")
        );
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM action_runs WHERE id = ?1",
                "pr-action-run"
            )
            .await
            .as_deref(),
            Some("complete"),
            "the pr node's blocked action run completes"
        );
        let observed = latest_resolution_attribution(db, "mr-pr-node")
            .await
            .expect("the observation is journaled");
        assert_eq!(observed.action, "merge");
        assert_eq!(observed.actor_kind, "unjournaled");
        assert_eq!(observed.surface, "github-refresh-observation");
        assert_eq!(observation_count(db).await, 1);

        let again = resolve_observed_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Merge,
            Landed {
                commit: Some("b77e7657"),
                source: None,
                resolved_at: None,
            },
            None,
        )
        .await
        .expect("a repeat observation is harmless");
        assert!(!again, "a resolved row is not resolved twice");
        assert_eq!(observation_count(db).await, 1, "journaled once");
        assert_eq!(row("CAST(merged_at AS TEXT)").await, merged_at);
    }

    /// GitHub closing a pull request says nothing about an issue somebody has
    /// already resolved, so an observed close never rewrites a merged issue.
    #[tokio::test(flavor = "current_thread")]
    async fn an_observed_close_leaves_an_already_resolved_issue_alone() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;
        crate::issues::status::resolve_terminal(
            &orch,
            db,
            "issue-pr-node",
            Resolution::Merged,
            crate::issues::status::StopFailure::Escalates,
        )
        .await
        .expect("the issue resolves merged");

        assert!(resolve_observed_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Close,
            Landed::default(),
            None,
        )
        .await
        .unwrap());

        assert_eq!(
            scalar(
                db,
                "SELECT status FROM merge_requests WHERE id = ?1",
                "mr-pr-node"
            )
            .await
            .as_deref(),
            Some("closed")
        );
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM issues WHERE id = ?1",
                "issue-pr-node"
            )
            .await
            .as_deref(),
            Some("merged"),
            "the observed close does not overwrite the issue's resolution"
        );
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM action_runs WHERE id = ?1",
                "pr-action-run"
            )
            .await
            .as_deref(),
            Some("complete"),
            "the pr node still resolves"
        );
    }

    /// A pull request a newer one superseded does not close the issue the newer
    /// one serves.
    #[tokio::test(flavor = "current_thread")]
    async fn an_observed_close_of_a_superseded_pull_request_keeps_the_issue_open() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        db.execute(
            "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES ('mr-newer', 'builder-job-2', 'proj-pr-node', 'issue-pr-node', 'Retry', 'agent/proj-3-builder-1', 'main', 'open', 5, 5)",
            (),
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;

        assert!(resolve_observed_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Close,
            Landed::default(),
            None,
        )
        .await
        .unwrap());

        assert_eq!(
            scalar(
                db,
                "SELECT status FROM merge_requests WHERE id = ?1",
                "mr-pr-node"
            )
            .await
            .as_deref(),
            Some("closed")
        );
        let issue_status = scalar(
            db,
            "SELECT status FROM issues WHERE id = ?1",
            "issue-pr-node",
        )
        .await
        .and_then(|status| status.parse::<crate::models::IssueStatus>().ok())
        .expect("the issue has a status");
        assert!(
            !issue_status.is_terminal(),
            "the issue stays unresolved, got {issue_status}"
        );
    }

    /// Cairn's own GitHub merge can be observed by a refresh before it resolves
    /// the row itself. The observation then owns the cascade, and Cairn's late
    /// resolution records only what it knows better: who acted, and the commit.
    #[tokio::test(flavor = "current_thread")]
    async fn a_cairn_merge_overtaken_by_its_observation_records_its_actor_and_commit() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;

        assert!(resolve_observed_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Merge,
            Landed::default(),
            None,
        )
        .await
        .unwrap());
        let merged_at = scalar(
            db,
            "SELECT CAST(merged_at AS TEXT) FROM merge_requests WHERE id = ?1",
            "mr-pr-node",
        )
        .await;

        resolve_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Merge,
            Some(PrResolutionAttribution::operator_ui()),
            Some("cafe1234"),
        )
        .await
        .expect("the late resolution lands");

        assert_eq!(
            scalar(
                db,
                "SELECT merged_commit FROM merge_requests WHERE id = ?1",
                "mr-pr-node"
            )
            .await
            .as_deref(),
            Some("cafe1234")
        );
        assert_eq!(
            scalar(
                db,
                "SELECT CAST(merged_at AS TEXT) FROM merge_requests WHERE id = ?1",
                "mr-pr-node"
            )
            .await,
            merged_at,
            "the row moved once"
        );
        let latest = latest_resolution_attribution(db, "mr-pr-node")
            .await
            .unwrap();
        assert_eq!(
            latest.actor_kind, "operator-ui",
            "the actor outranks the observation"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pr_resolution_completes_pr_action_run_for_producing_job_owner() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;

        let execution_id = complete_pr_action_run_if_owner(&db, "builder-job", 42)
            .await
            .unwrap()
            .expect("builder job resolves its pr action_run child");

        assert_eq!(execution_id, "exec-pr-node");
        let status = db
            .query_text(
                "SELECT status FROM action_runs WHERE id = 'pr-action-run'",
                (),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status, "complete");
    }

    /// The merge/close lifecycle transition itself must land in the owning replica.
    /// A team-prefixed producing job resolves (Close) against its injected replica
    /// — marking the team MR closed and never touching the empty private DB — the
    /// state-transition step that runs AFTER the irreversible GitHub/local work.
    #[tokio::test(flavor = "current_thread")]
    async fn resolve_pr_node_close_routes_to_owning_team_replica() {
        use crate::models::{
            ExecutionSnapshot, RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
        };
        use crate::pr_data::actions::test_support::test_orchestrator;
        use crate::services::testing::MockGitClient;
        use std::collections::HashMap;
        use std::sync::Arc;

        let orch = test_orchestrator(migrated_db().await, MockGitClient::new());
        let team = Arc::new(migrated_db().await);
        orch.db.insert_team_db_for_test("team1", team.clone()).await;

        let exec_id = "team1~00000000-0000-4000-8000-0000000000e0";
        let job_id = "team1~00000000-0000-4000-8000-0000000000e1";
        let issue_id = "team1~00000000-0000-4000-8000-0000000000e2";
        let mr_id = "team1~00000000-0000-4000-8000-0000000000e3";
        let snapshot = ExecutionSnapshot::new(
            RecipeSnapshot {
                id: "recipe-1".into(),
                name: "R".into(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: Vec::new(),
                edges: Vec::new(),
            },
            HashMap::new(),
            HashMap::new(),
            TriggerContext {
                issue_id: Some(issue_id.to_string()),
                project_id: "proj-t".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
        )
        .to_json()
        .unwrap();

        // Seed the full close fixture in the TEAM replica ONLY; the private DB
        // stays empty, so a resolution that read `orch.db.local` would error.
        {
            let exec_id = exec_id.to_string();
            let job_id = job_id.to_string();
            let issue_id = issue_id.to_string();
            let mr_id = mr_id.to_string();
            team.write(|conn| {
                let exec_id = exec_id.clone();
                let job_id = job_id.clone();
                let issue_id = issue_id.clone();
                let mr_id = mr_id.clone();
                let snapshot = snapshot.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                         VALUES ('proj-t', 'default', 'P', 'proj', '/repo', 'main', 1, 1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                         VALUES (?1, 'proj-t', 1, 'Issue', 'active', 1, 1)",
                        params![issue_id.as_str()],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, snapshot, started_at, seq)
                         VALUES (?1, 'recipe-1', ?2, 'proj-t', 'running', ?3, 1, 1)",
                        params![exec_id.as_str(), issue_id.as_str(), snapshot.as_str()],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, branch, created_at, updated_at)
                         VALUES (?1, ?2, ?3, 'proj-t', 'complete', 'feature', 1, 1)",
                        params![job_id.as_str(), exec_id.as_str(), issue_id.as_str()],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
                         VALUES (?1, ?2, 'proj-t', ?3, 'Team PR', 'feature', 'main', 'open', 1, 1)",
                        params![mr_id.as_str(), job_id.as_str(), issue_id.as_str()],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        }

        resolve_pr_node(&orch, job_id, PrNodeResolution::Close, None, None)
            .await
            .expect("a team PR resolves its close transition against the injected replica");

        // The transition landed in the TEAM replica, not the (empty) private DB.
        let team_status = team
            .query_text(
                "SELECT status FROM merge_requests WHERE id = ?1",
                params![mr_id],
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(team_status, "closed");
        let private_status = orch
            .db
            .local
            .query_opt_text(
                "SELECT status FROM merge_requests WHERE id = ?1",
                params![mr_id],
            )
            .await
            .unwrap();
        assert!(
            private_status.is_none(),
            "the private DB must never receive the team PR's row"
        );
    }

    /// A resolution names the execution it resolved and records what its jobs
    /// were doing, but copies none of the execution's snapshot: the recipe
    /// graph, prompts and skills stay on the execution.
    #[tokio::test(flavor = "current_thread")]
    async fn a_resolution_names_its_execution_and_copies_no_snapshot() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        db.execute(
            "UPDATE merge_requests SET head_sha = 'feedfacecafe0123' WHERE id = 'mr-pr-node'",
            (),
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;

        resolve_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Merge,
            Some(PrResolutionAttribution::operator_ui()),
            None,
        )
        .await
        .unwrap();

        let stored = scalar(
            db,
            "SELECT lane_snapshot FROM pr_resolution_attributions WHERE merge_request_id = ?1",
            "mr-pr-node",
        )
        .await
        .expect("the resolution journals its lane snapshot");
        for snapshot_field in ["executionSnapshot", "recipe", "triggerContext", "edges"] {
            assert!(
                !stored.contains(snapshot_field),
                "{snapshot_field} copied into {stored}"
            );
        }
        let snapshot = LaneSnapshot::parse(&stored);
        let execution = snapshot.execution.expect("the execution is named");
        assert_eq!(execution.id.as_deref(), Some("exec-pr-node"));
        assert!(execution.execution_status.is_some());
        assert!(
            execution
                .jobs
                .iter()
                .any(|job| job.status.as_deref() == Some("complete")),
            "{:?}",
            execution.jobs
        );
        assert_eq!(snapshot.head.as_deref(), Some("feedfacecafe0123"));
    }

    fn lanes_at_resolution() -> Vec<crate::execution::checks_status::NodeCheckStatus> {
        use crate::execution::checks_status::{test_status, NodeCheckState, VerifiedHead};
        let mut failed = test_status("rust-tests", "advisory", "review", NodeCheckState::Failed);
        failed.passed = Some(120);
        failed.failed = Some(3);
        failed.ran_at = Some(1_700_000_000);
        failed.output_tail = Some("x".repeat(50_000));
        failed.failure_names = vec!["crate::module::a_failing_test".to_string()];
        let mut replayed = test_status("lint", "advisory", "review", NodeCheckState::Passed);
        replayed.verified_on = Some(VerifiedHead {
            head: "0123456789abcdef".to_string(),
            base: "basebasebase".to_string(),
        });
        vec![
            failed,
            replayed,
            test_status("api", "gate", "write", NodeCheckState::Passed),
            test_status("rust-lint", "advisory", "review", NodeCheckState::Pending),
            test_status("typecheck", "advisory", "review", NodeCheckState::Running),
        ]
    }

    fn attribution_with(lane_snapshot: LaneSnapshot) -> StoredPrResolutionAttribution {
        StoredPrResolutionAttribution {
            action: "merge".to_string(),
            actor_kind: "operator-ui".to_string(),
            actor_identity: None,
            surface: "operator-ui".to_string(),
            created_at: 1_700_000_000,
            lane_snapshot,
        }
    }

    /// A review lane still waiting on the wave this resolution cancels is named
    /// as cancelled by it, with the wave's attempt. A lane no wave of this
    /// resolution was running still reads as having had no verdict yet, and
    /// neither is ever "not applicable", which only the impact gate decides.
    #[test]
    fn a_lane_the_resolution_cancels_is_named_as_cancelled_not_inapplicable() {
        use crate::execution::checks_status::{test_status, NodeCheckState};
        let mut snapshot = LaneSnapshot {
            execution: None,
            head: Some("feedfacecafe0123".to_string()),
            systematic_checks: [
                test_status("rust-lint", "advisory", "review", NodeCheckState::Passed),
                test_status("rust-tests", "advisory", "review", NodeCheckState::Running),
                test_status(
                    "dead-code",
                    "advisory",
                    "review",
                    NodeCheckState::NotApplicable,
                ),
                test_status("api", "gate", "write", NodeCheckState::Pending),
            ]
            .iter()
            .filter_map(LaneAtResolution::from_status)
            .collect(),
        };
        record_cancelled_wave(&mut snapshot, "wave-1");

        let stored = serde_json::to_string(&snapshot).unwrap();
        assert_eq!(LaneSnapshot::parse(&stored), snapshot);
        let cancelled = snapshot
            .systematic_checks
            .iter()
            .filter(|lane| lane.cancelled_by.is_some())
            .map(|lane| lane.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            cancelled,
            ["rust-tests"],
            "only a review lane waits on the wave"
        );

        let rendered =
            render_resolution_provenance(&attribution_with(snapshot), &ProvenanceLinks::default());
        assert!(
            rendered.contains(
                "| rust-tests | running, no verdict; cancelled by this resolution (wave `wave-1`) |"
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains("| none: cancelled by this resolution |"),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "This resolution cancelled review wave `wave-1` while these lanes waited on it: rust-tests."
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains("| api | pending, no verdict |"),
            "{rendered}"
        );
        assert!(rendered.contains("| rust-lint | passed |"), "{rendered}");
        assert!(
            rendered.contains("| dead-code | not applicable |"),
            "{rendered}"
        );
        assert!(
            rendered.contains("2 of 4 check lanes had no verdict at resolution: rust-tests, api."),
            "{rendered}"
        );
    }

    /// A row journaled before the cancellation was recorded parses with none,
    /// and renders exactly as it did: it claims nothing it did not record.
    #[test]
    fn a_row_without_a_recorded_cancellation_renders_as_it_was_journaled() {
        let stored = r#"{"head":"feedfacecafe0123","systematicChecks":[{"name":"rust-tests","state":"notApplicable","when":"review","observation":{"kind":"absent","reason":"the check does not apply"}}]}"#;
        let snapshot = LaneSnapshot::parse(stored);
        assert_eq!(snapshot.systematic_checks[0].cancelled_by, None);
        let rendered =
            render_resolution_provenance(&attribution_with(snapshot), &ProvenanceLinks::default());
        assert!(
            rendered.contains("| rust-tests | not applicable |"),
            "{rendered}"
        );
        assert!(!rendered.contains("cancelled"), "{rendered}");
    }

    /// The stored record keeps each lane's state, policy, cadence, counts and
    /// verified head, and drops the detail a lane carries for live surfaces.
    #[test]
    fn the_lane_record_keeps_each_lanes_state_and_drops_its_detail() {
        let snapshot = LaneSnapshot {
            execution: Some(ExecutionAtResolution {
                id: Some("exec-1".to_string()),
                execution_status: Some("running".to_string()),
                jobs: vec![JobAtResolution {
                    node: Some("builder".to_string()),
                    status: Some("complete".to_string()),
                }],
            }),
            head: Some("feedfacecafe0123".to_string()),
            systematic_checks: lanes_at_resolution()
                .iter()
                .filter_map(LaneAtResolution::from_status)
                .collect(),
        };
        let stored = serde_json::to_string(&snapshot).unwrap();

        assert!(stored.len() < 2_048, "{} bytes: {stored}", stored.len());
        assert!(!stored.contains("xxxx"), "the output tail is not stored");
        assert!(
            !stored.contains("a_failing_test"),
            "failing tests are read through the observation"
        );
        assert_eq!(LaneSnapshot::parse(&stored), snapshot);
        let states = snapshot
            .systematic_checks
            .iter()
            .map(|lane| (lane.name.as_str(), lane.state.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            states,
            [
                ("rust-tests", "failed"),
                ("lint", "passed"),
                ("api", "passed"),
                ("rust-lint", "pending"),
                ("typecheck", "running"),
            ]
        );
        let failed = &snapshot.systematic_checks[0];
        assert_eq!((failed.passed, failed.failed), (Some(120), Some(3)));
        assert_eq!(failed.policy.as_deref(), Some("advisory"));
        assert_eq!(failed.when.as_deref(), Some("review"));
        assert_eq!(
            snapshot.systematic_checks[1]
                .verified_on
                .as_ref()
                .map(|verified| verified.head.as_str()),
            Some("0123456789abcdef")
        );
    }

    /// The provenance block is a lane table that says which lanes had no
    /// verdict, with links for the rest, never a raw JSON line.
    #[test]
    fn provenance_renders_the_lanes_as_a_table_with_links() {
        let attribution = attribution_with(LaneSnapshot {
            execution: Some(ExecutionAtResolution {
                id: Some("exec-1".to_string()),
                execution_status: Some("running".to_string()),
                jobs: vec![JobAtResolution {
                    node: Some("builder".to_string()),
                    status: Some("complete".to_string()),
                }],
            }),
            head: Some("feedfacecafe0123".to_string()),
            systematic_checks: lanes_at_resolution()
                .iter()
                .filter_map(LaneAtResolution::from_status)
                .collect(),
        });
        let rendered = render_resolution_provenance(
            &attribution,
            &ProvenanceLinks {
                checks: Some("cairn://p/proj/3/1/builder/checks".to_string()),
                execution: Some("cairn://p/proj/3/1".to_string()),
            },
        );

        assert!(
            rendered.starts_with("Merged by operator (UI) at "),
            "{rendered}"
        );
        assert!(rendered.contains("Surface: `operator-ui`"), "{rendered}");
        assert!(rendered.contains("Head: `feedfacecafe`"), "{rendered}");
        assert!(
            rendered.contains("Execution: running; jobs: builder complete"),
            "{rendered}"
        );
        assert!(
            rendered
                .contains("2 of 5 check lanes had no verdict at resolution: rust-lint, typecheck."),
            "{rendered}"
        );
        assert!(rendered.contains("| Lane | Verdict | Head | Observation |\n|---|---|---|---|\n"));
        assert!(
            rendered.contains("| rust-tests | failed (120 passed, 3 failed) | `feedfacecafe` |"),
            "{rendered}"
        );
        assert!(
            rendered.contains("| lint | passed | `0123456789ab` |"),
            "a replayed verdict shows the head it was recorded at: {rendered}"
        );
        assert!(rendered.contains("| rust-lint | pending, no verdict | `feedfacecafe` |"));
        assert!(rendered.contains(
            "The rest: lane detail: cairn://p/proj/3/1/builder/checks; execution: cairn://p/proj/3/1."
        ));
        assert!(!rendered.contains('{'), "no JSON in the block: {rendered}");
        assert!(rendered.ends_with("</details>\n"));
    }

    /// However many lanes a project configures, the block stays a summary.
    #[test]
    fn provenance_stays_under_its_size_bound() {
        use crate::execution::checks_status::{test_status, NodeCheckState};
        let lanes = (0..200)
            .map(|n| {
                let mut lane = test_status(
                    &format!("a-fairly-long-check-lane-name-{n:03}"),
                    "advisory",
                    "review",
                    if n % 2 == 0 {
                        NodeCheckState::Failed
                    } else {
                        NodeCheckState::Pending
                    },
                );
                lane.passed = Some(10_000);
                lane.failed = Some(1_000);
                lane.skipped = Some(100);
                lane.output_tail = Some("y".repeat(10_000));
                lane
            })
            .collect::<Vec<_>>();
        let attribution = attribution_with(LaneSnapshot {
            execution: Some(ExecutionAtResolution {
                id: Some("exec".to_string()),
                execution_status: Some("running".to_string()),
                jobs: (0..100)
                    .map(|n| JobAtResolution {
                        node: Some(format!("a-fairly-long-node-name-{n:03}")),
                        status: Some("complete".to_string()),
                    })
                    .collect(),
            }),
            head: Some("feedfacecafe0123".to_string()),
            systematic_checks: lanes
                .iter()
                .filter_map(LaneAtResolution::from_status)
                .map(|mut lane| {
                    if lane.state == "failed" {
                        lane.failure = Some(ObservedFailure {
                            kind: Some("timed_out".to_string()),
                            names: vec![format!("module::failing_{}", "z".repeat(400))],
                            total: 1_000,
                        });
                    }
                    lane
                })
                .collect(),
        });
        let rendered = render_resolution_provenance(&attribution, &ProvenanceLinks::default());
        assert!(
            rendered.contains("99995 more failing test names are not listed."),
            "{rendered}"
        );
        assert!(
            rendered.len() < PROVENANCE_BYTES,
            "{} bytes rendered for 200 lanes",
            rendered.len()
        );
        // The byte budget lists fewer rows than the row cap once five long
        // failing names and failure kinds share the block; the omitted count
        // is exactly what the table left out, and the failures still show.
        let rows = rendered
            .lines()
            .filter(|line| line.starts_with("| a-fairly-long-check-lane-name-"))
            .count();
        assert!(rows > 41 && rows < PROVENANCE_LANE_ROWS, "{rows} rows");
        assert!(
            rendered.contains(&format!("\n{} more lanes are not listed.\n", 200 - rows)),
            "{rendered}"
        );
        assert_eq!(
            rendered.matches("Failing test (").count(),
            PROVENANCE_LINE_NAMES
        );
        assert!(rendered.contains("failed (timed_out) ("), "{rendered}");
        assert!(rendered.ends_with("</details>\n"));
        // A reader greps the artifact for a lane: every line stays short, and
        // the lane's own row is what the grep returns.
        let longest = rendered.lines().map(str::len).max().unwrap_or(0);
        assert!(longest < 300, "a {longest}-byte line:\n{rendered}");
        let hits = rendered
            .lines()
            .filter(|line| line.contains("a-fairly-long-check-lane-name-041"))
            .collect::<Vec<_>>();
        assert_eq!(hits.len(), 1, "{hits:?}");
        assert!(
            hits[0].starts_with("| a-fairly-long-check-lane-name-041 |"),
            "{hits:?}"
        );
        assert!(rendered.contains("and 95 more"), "{rendered}");
    }

    #[test]
    fn lanes_without_verdict_do_not_render_stale_result_facts() {
        for state in ["pending", "running"] {
            let mut lane: LaneAtResolution = serde_json::from_value(serde_json::json!({
                "name": "tests", "state": state,
                "passed": 10000, "failed": 1000, "skipped": 100
            }))
            .unwrap();
            lane.failure = Some(ObservedFailure {
                kind: Some("infrastructure".to_string()),
                ..ObservedFailure::default()
            });
            assert_eq!(lane.verdict(), format!("{state}, no verdict"));
        }
    }

    /// A red lane's failing tests are read through the observation it cites,
    /// one shared budget across lanes, each name cut short and escaped.
    #[tokio::test(flavor = "current_thread")]
    async fn a_cited_red_lane_renders_its_observations_failures_within_a_bound() {
        let db = migrated_db().await;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('p-obs', 'default', 'Project', 'proj', '/repo', 'main', 1, 1);
             INSERT INTO check_result_observations
               (id, project_id, commit_sha, tree_hash, check_name, input_hash, environment_fingerprint,
                exit_code, verdict, failure_kind, complete, reusable, parser_version, result_schema_version,
                ran_at, duration_ms, cadence, output_tail)
             VALUES ('obs-red', 'p-obs', 'c', 't', 'rust-tests', 'i', 'env', 1, 'failed', 'test', 1, 0, 1, 1,
                     1, 1, 'review', 'TAIL-NOT-RENDERED'),
                    ('obs-two', 'p-obs', 'c', 't', 'api', 'i', 'env', 1, 'failed', NULL, 1, 0, 1, 1,
                     1, 1, 'review', 'tail'),
                    ('obs-green', 'p-obs', 'c', 't', 'lint', 'i', 'env', 0, 'passed', NULL, 1, 1, 1, 1,
                     1, 1, 'review', 'ok');
             INSERT INTO check_test_results (observation_id, test_id, status)
               VALUES ('obs-red', 'passing_test', 'passed'),
                      ('obs-two', 'another_lane', 'failed'),
                      ('obs-green', 'green_test', 'passed');",
        )
        .await
        .unwrap();
        let names = (0..20)
            .map(|n| format!("test_{n:02}|\n{}", "é".repeat(200)))
            .collect::<Vec<_>>();
        for name in &names {
            db.execute(
                "INSERT INTO check_test_results (observation_id, test_id, status, failure_excerpt)
                 VALUES ('obs-red', ?1, 'failed', 'EXCERPT-NOT-RENDERED')",
                params![name.as_str()],
            )
            .await
            .unwrap();
        }
        let mut snapshot = LaneSnapshot::parse(
            &serde_json::json!({"systematicChecks": [
                {"name": "rust-tests", "state": "failed", "failed": 20,
                 "observation": {"kind": "cited", "id": "obs-red"}},
                {"name": "api", "state": "failed", "failed": 1,
                 "observation": {"kind": "cited", "id": "obs-two"}},
                {"name": "lint", "state": "passed",
                 "observation": {"kind": "cited", "id": "obs-green"}},
                {"name": "gone", "state": "failed",
                 "observation": {"kind": "cited", "id": "no-such-observation"}},
                {"name": "web", "state": "failed",
                 "observation": {"kind": "absent", "reason": "explicit reason"}},
                {"name": "stale", "state": "running",
                 "observation": {"kind": "cited", "id": "obs-red"}}
            ]})
            .to_string(),
        );
        let stored = serde_json::to_string(&snapshot).unwrap();
        hydrate_lane_failures(&db, &mut snapshot).await;

        let lanes = &snapshot.systematic_checks;
        assert_eq!(
            lanes[0].failure,
            Some(ObservedFailure {
                kind: Some("test".to_string()),
                names: names[..PROVENANCE_LINE_NAMES].to_vec(),
                total: 20,
            })
        );
        assert_eq!(
            lanes[1].failure,
            Some(ObservedFailure {
                kind: None,
                names: Vec::new(),
                total: 1,
            }),
            "the name budget is spent, the count is not"
        );
        assert_eq!(
            lanes[2].failure, None,
            "a passed observation has no failure"
        );
        assert_eq!(lanes[3].failure, None);
        assert_eq!(
            lanes[3].observation,
            Some(LaneObservation::Cited {
                id: "no-such-observation".to_string(),
                uri: None
            })
        );
        assert_eq!(lanes[4].failure, None);
        assert_eq!(
            lanes[5].failure, None,
            "a lane with no verdict shows no failure"
        );
        assert_eq!(
            serde_json::to_string(&snapshot).unwrap(),
            stored,
            "hydrated detail is never written back"
        );

        let rendered =
            render_resolution_provenance(&attribution_with(snapshot), &ProvenanceLinks::default());
        assert!(
            rendered.contains("| rust-tests | failed (test) (20 failed) |"),
            "{rendered}"
        );
        assert!(rendered.contains("observation `obs-red`"), "{rendered}");
        assert!(rendered.contains("none: explicit reason"), "{rendered}");
        assert_eq!(
            rendered.matches("Failing test (rust-tests): ").count(),
            PROVENANCE_LINE_NAMES,
            "{rendered}"
        );
        assert!(rendered.contains("test_00\\| "), "{rendered}");
        assert!(rendered.contains('…'), "{rendered}");
        assert!(
            rendered.contains("16 more failing test names are not listed."),
            "{rendered}"
        );
        for absent in [
            "test_05",
            "another_lane",
            "passing_test",
            "green_test",
            "EXCERPT-NOT-RENDERED",
            "TAIL-NOT-RENDERED",
        ] {
            assert!(!rendered.contains(absent), "{absent} in {rendered}");
        }
        assert!(rendered.len() < 4_096, "{} bytes", rendered.len());
        assert!(rendered.lines().all(|line| line.len() < 300));
    }

    /// A row journaled before the lane record existed still renders as a lane
    /// table: the copied execution snapshot and each lane's detail are skipped.
    #[test]
    fn a_legacy_lane_snapshot_renders_as_a_table() {
        let legacy = serde_json::json!({
            "execution": {
                "executionStatus": "complete",
                "executionSnapshot": {"recipe": {"name": "A recipe"}, "agents": {"builder": {"prompt": "p".repeat(40_000)}}},
                "jobs": [{"node": "builder", "status": "complete"}, {"node": null, "status": "blocked"}]
            },
            "systematicChecks": [
                {"jobId": "j", "requestId": null, "name": "rust-tests", "state": "pending", "policy": "advisory", "when": "review", "cached": null, "failureNames": [], "outputTail": null},
                {"jobId": "j", "name": "api", "state": "passed", "policy": "gate", "when": "write", "passed": 47, "failureNames": [], "outputTail": "ok"}
            ]
        })
        .to_string();
        let snapshot = LaneSnapshot::parse(&legacy);
        assert_eq!(snapshot.systematic_checks.len(), 2);
        let rendered =
            render_resolution_provenance(&attribution_with(snapshot), &ProvenanceLinks::default());
        assert!(!rendered.contains("executionSnapshot"), "{rendered}");
        assert!(!rendered.contains("ppppp"), "{rendered}");
        assert!(rendered.contains("Execution: complete; jobs: builder complete, (unnamed) blocked"));
        assert!(
            rendered.contains("| rust-tests | pending, no verdict | — |"),
            "{rendered}"
        );
        assert!(
            rendered.contains("| api | passed (47 passed) | — |"),
            "{rendered}"
        );
        assert!(rendered.len() < 1_024, "{rendered}");

        // The unreadable shapes an earlier build could journal read as empty.
        assert_eq!(LaneSnapshot::parse("not json"), LaneSnapshot::default());
        assert_eq!(
            LaneSnapshot::parse(r#"{"execution":{"state":"unavailable"},"systematicChecks":[]}"#),
            LaneSnapshot::default()
        );
    }

    /// A lane cites the observation its verdict came from: by address when the
    /// observation has a public handle, by id when it predates handles. A lane
    /// with no observation says why instead of leaving the pointer empty.
    #[tokio::test(flavor = "current_thread")]
    async fn each_lane_cites_its_observation_or_says_why_it_has_none() {
        let db = migrated_db().await;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('p-obs', 'default', 'Project', 'proj', '/repo', 'main', 1, 1);
             INSERT INTO check_result_observations
               (id, project_id, commit_sha, tree_hash, check_name, input_hash, environment_fingerprint,
                exit_code, verdict, complete, reusable, parser_version, result_schema_version,
                ran_at, duration_ms, cadence, output_tail, public_handle)
             VALUES ('obs-red', 'p-obs', 'c', 't', 'rust-tests', 'i', 'env', 1, 'failed', 1, 0, 1, 1,
                     1, 1, 'review', 'the tail', 'e884003b'),
                    ('obs-old', 'p-obs', 'c', 't', 'lint', 'i', 'env', 0, 'passed', 1, 1, 1, 1,
                     1, 1, 'write', 'ok', NULL);",
        )
        .await
        .unwrap();

        assert_eq!(
            lane_observation(&db, Some("obs-red"), "failed").await,
            LaneObservation::Cited {
                id: "obs-red".to_string(),
                uri: Some("cairn://p/proj/check-observations/e884003b".to_string()),
            }
        );
        assert_eq!(
            lane_observation(&db, Some("obs-old"), "passed").await,
            LaneObservation::Cited {
                id: "obs-old".to_string(),
                uri: None,
            }
        );
        assert_eq!(
            lane_observation(&db, None, "pending").await,
            LaneObservation::Absent {
                reason: "no verdict at resolution".to_string(),
            }
        );
        assert_eq!(
            lane_observation(&db, None, "notApplicable").await,
            LaneObservation::Absent {
                reason: "the check does not apply".to_string(),
            }
        );

        let mut lanes = lanes_at_resolution()
            .iter()
            .filter_map(LaneAtResolution::from_status)
            .collect::<Vec<_>>();
        lanes[0].observation = Some(lane_observation(&db, Some("obs-red"), "failed").await);
        lanes[3].observation = Some(lane_observation(&db, None, "pending").await);
        let snapshot = LaneSnapshot {
            execution: None,
            head: Some("feedfacecafe0123".to_string()),
            systematic_checks: lanes,
        };
        let stored = serde_json::to_string(&snapshot).unwrap();
        assert!(
            stored.contains(r#""observation":{"kind":"cited","id":"obs-red""#),
            "{stored}"
        );
        assert!(
            stored
                .contains(r#""observation":{"kind":"absent","reason":"no verdict at resolution"}"#),
            "{stored}"
        );
        assert_eq!(LaneSnapshot::parse(&stored), snapshot);

        let rendered =
            render_resolution_provenance(&attribution_with(snapshot), &ProvenanceLinks::default());
        assert!(
            rendered.contains(
                "| rust-tests | failed (120 passed, 3 failed) | `feedfacecafe` | cairn://p/proj/check-observations/e884003b |"
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "| rust-lint | pending, no verdict | `feedfacecafe` | none: no verdict at resolution |"
            ),
            "{rendered}"
        );
    }

    /// The migration strips the copied execution snapshot and each lane's
    /// detail from rows journaled before, and keeps every lane's state.
    #[tokio::test]
    async fn the_migration_shrinks_journaled_lane_snapshots() {
        use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(dir.path().join("lane-snapshots.db"))
            .await
            .unwrap();
        let before = TURSO_MIGRATIONS
            .iter()
            .take_while(|migration| migration.name() != "trim_pr_resolution_lane_snapshot")
            .copied()
            .collect::<Vec<_>>();
        assert!(
            before.len() < TURSO_MIGRATIONS.len(),
            "the migration must be in the lineage"
        );
        MigrationRunner::new(before).run(&db).await.unwrap();
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES('p', 'w', 'P', 'p', '/tmp/p', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES('i', 'p', 1, 'I', 'merged', 'merged', 'none', 1, 1);
            INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
             VALUES('j', 'p', 'i', 'complete', 1, 1);
            INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES('mr', 'j', 'p', 'i', 'PR', 'feature', 'main', 'merged', 1, 2);
            ",
        )
        .await
        .unwrap();
        let legacy = serde_json::json!({
            "execution": {
                "executionStatus": "complete",
                "executionSnapshot": {"recipe": {"name": "A recipe"}, "agents": {"builder": {"prompt": "p".repeat(40_000)}}},
                "jobs": [{"node": "builder", "status": "complete"}]
            },
            "systematicChecks": [
                {"jobId": "j", "requestId": "r", "name": "rust-tests", "state": "failed", "policy": "advisory", "when": "review", "passed": 120, "failed": 3, "failureNames": ["a::b"], "outputTail": "t".repeat(5_000), "verifiedOn": {"head": "0123456789abcdef", "base": "basebase"}},
                {"jobId": "j", "name": "rust-lint", "state": "pending", "policy": "advisory", "when": "review", "failureNames": []}
            ]
        })
        .to_string();
        let unavailable = r#"{"execution":{"state":"unavailable"},"systematicChecks":[]}"#;
        for (id, lane_snapshot) in [
            ("legacy", legacy.as_str()),
            ("unavailable", unavailable),
            ("unreadable", "not json"),
        ] {
            db.execute(
                "INSERT INTO pr_resolution_attributions
                   (id, merge_request_id, action, actor_kind, surface, lane_snapshot, created_at)
                 VALUES (?1, 'mr', 'merge', 'operator-ui', 'operator-ui', ?2, 2)",
                params![id, lane_snapshot],
            )
            .await
            .unwrap();
        }
        let expected_lanes = LaneSnapshot::parse(&legacy).systematic_checks;

        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();

        let stored = |id: &'static str| {
            let db = &db;
            async move {
                db.query_opt_text(
                    "SELECT lane_snapshot FROM pr_resolution_attributions WHERE id = ?1",
                    params![id],
                )
                .await
                .unwrap()
                .unwrap()
            }
        };
        let shrunk = stored("legacy").await;
        assert!(shrunk.len() < 1_024, "{} bytes: {shrunk}", shrunk.len());
        for dropped in [
            "executionSnapshot",
            "outputTail",
            "failureNames",
            "jobId",
            "basebase",
        ] {
            assert!(!shrunk.contains(dropped), "{dropped} kept in {shrunk}");
        }
        let snapshot = LaneSnapshot::parse(&shrunk);
        assert_eq!(snapshot.systematic_checks, expected_lanes);
        assert_eq!(snapshot.systematic_checks.len(), 2);
        let execution = snapshot.execution.expect("the execution's state is kept");
        assert_eq!(execution.execution_status.as_deref(), Some("complete"));
        assert_eq!(execution.jobs.len(), 1);
        assert_eq!(stored("unavailable").await, unavailable);
        assert_eq!(stored("unreadable").await, "not json");
    }
}
