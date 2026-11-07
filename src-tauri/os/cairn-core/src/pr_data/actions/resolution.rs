//! PR-node resolution: the merge / close lifecycle (`resolve_pr_node`), the
//! `merge_requests` status transitions, and downstream execution advancement.

use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use crate::transitions::Resolution;
use cairn_db::turso::params;

use super::context::{
    db_error, resolve_merge_mr_context_for_job, MergeMrContext, PrNodeResolution,
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
/// reads both shapes and keeps only these fields.
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
/// `pending` and `running` mean the lane had no verdict yet.
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
    let uri = db
        .query_opt(
            "SELECT p.key, o.public_handle
               FROM check_result_observations o
               JOIN projects p ON p.id = o.project_id
              WHERE o.id = ?1",
            (id.to_string(),),
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await
        .ok()
        .flatten()
        .and_then(|(key, handle)| {
            handle.map(|handle| format!("cairn://p/{key}/check-observations/{handle}"))
        });
    LaneObservation::Cited {
        id: id.to_string(),
        uri,
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
        let state = match self.state.as_str() {
            "pending" | "running" => format!("{}, no verdict", self.state),
            "notApplicable" => "not applicable".to_string(),
            state => state.to_string(),
        };
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
        out.push_str("\n| Lane | Verdict | Head | Observation |\n|---|---|---|---|\n");
        for lane in lanes.iter().take(PROVENANCE_LANE_ROWS) {
            let head = lane
                .verified_on
                .as_ref()
                .map(|verified| verified.head.as_str())
                .or(snapshot.head.as_deref())
                .map(|head| format!("`{}`", short_head(head)))
                .unwrap_or_else(|| "—".to_string());
            out.push_str(&format!(
                "| {} | {} | {head} | {} |\n",
                table_cell(&lane.name),
                table_cell(&lane.verdict()),
                table_cell(&LaneObservation::cell(lane.observation.as_ref()))
            ));
        }
        if lanes.len() > PROVENANCE_LANE_ROWS {
            out.push_str(&format!(
                "\n{} more lanes are not listed.\n",
                lanes.len() - PROVENANCE_LANE_ROWS
            ));
        }
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
    let statuses = crate::execution::checks_status::node_check_statuses(orch, job_id)
        .await
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

pub async fn latest_resolution_attribution(
    db: &LocalDb,
    mr_id: &str,
) -> Option<StoredPrResolutionAttribution> {
    let mr_id = mr_id.to_string();
    // A Cairn merge can land on GitHub and be observed by a refresh before its
    // own resolution journals, inside the same second. The principal that acted
    // outranks the observation of its act.
    db.query_opt("SELECT action, actor_kind, actor_identity, surface, created_at, lane_snapshot FROM pr_resolution_attributions WHERE merge_request_id = ?1 ORDER BY created_at DESC, CASE WHEN actor_kind = 'unjournaled' THEN 1 ELSE 0 END LIMIT 1", (mr_id,), |row| Ok(StoredPrResolutionAttribution { action: row.text(0)?, actor_kind: row.text(1)?, actor_identity: row.opt_text(2)?, surface: row.text(3)?, created_at: row.i64(4)?, lane_snapshot: LaneSnapshot::parse(&row.text(5)?) })).await.ok().flatten()
}

/// Move a `merge_requests` row to its terminal status, returning whether this
/// call moved it.
///
/// The conditional update is the claim on the resolution. A refresh that saw
/// GitHub's merge and Cairn's own merge path can both reach one row, as can two
/// readers that shared one refresh; only the call that moves the row runs the
/// terminal cascade. A closed row may still become merged, because GitHub can
/// merge a pull request that was closed and reopened. Nothing moves a merged row.
async fn claim_merge_request_resolution(
    db: &LocalDb,
    mr_id: &str,
    resolution: PrNodeResolution,
    merged_commit: Option<&str>,
    now: i64,
) -> Result<bool, String> {
    let moved = match resolution {
        PrNodeResolution::Merge => {
            db.execute(
                "UPDATE merge_requests
                 SET status = 'merged', merged_at = ?1, updated_at = ?1,
                     merged_commit = COALESCE(?2, merged_commit)
                 WHERE id = ?3 AND status IN ('open', 'closed')",
                params![now, merged_commit, mr_id],
            )
            .await
        }
        PrNodeResolution::Close => {
            db.execute(
                "UPDATE merge_requests
                 SET status = 'closed', closed_at = ?1, updated_at = ?1
                 WHERE id = ?2 AND status = 'open'",
                params![now, mr_id],
            )
            .await
        }
    }
    .map_err(|e| db_error("Failed to update merge request", e))?;
    Ok(moved > 0)
}

/// Whether an observed resolution of `mr_id` speaks for its issue: the issue is
/// still unresolved, and no other pull request of the issue is open or newer.
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
        .is_ok_and(|status| !status.is_terminal()))
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
) -> Result<(), String> {
    crate::issues::status::resolve_terminal(
        orch,
        db,
        issue_id,
        resolution,
        crate::issues::status::StopFailure::Escalates,
    )
    .await
    .map_err(|refusal| refusal.to_string())
}

/// Who brought a pull request to its terminal state.
enum Resolver {
    /// A Cairn action that performed the merge or close. `None` is a boundary
    /// that did not say whom it acted for, which is journaled loudly.
    Performed(Option<PrResolutionAttribution>),
    /// A refresh that saw GitHub report a merge or close Cairn did not perform.
    Observed,
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
        merged_commit,
        Resolver::Performed(attribution),
    )
    .await
    .map(|_| ())
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
    merged_commit: Option<&str>,
) -> Result<bool, String> {
    resolve_pr_node_as(
        orch,
        owner_id,
        resolution,
        merged_commit,
        Resolver::Observed,
    )
    .await
}

async fn resolve_pr_node_as(
    orch: &Orchestrator,
    owner_id: &str,
    resolution: PrNodeResolution,
    merged_commit: Option<&str>,
    resolver: Resolver,
) -> Result<bool, String> {
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
    let merge_context = resolve_merge_mr_context_for_job(&db, owner_id).await?;
    let mr_id = merge_context.mr.mr_id.clone();
    let now = chrono::Utc::now().timestamp();
    let lane_snapshot =
        serde_json::to_string(&lane_snapshot_for_job(orch, &db, owner_id, &mr_id).await)
            .map_err(|e| format!("Failed to record the lane snapshot: {e}"))?;
    let performed = matches!(resolver, Resolver::Performed(_));
    let claimed =
        claim_merge_request_resolution(&db, &mr_id, resolution, merged_commit, now).await?;
    let attribution = match resolver {
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
        Resolver::Observed => claimed.then(PrResolutionAttribution::github_observation),
    };
    if let Some(attribution) = attribution {
        db.execute(
            "INSERT INTO pr_resolution_attributions (id, merge_request_id, action, actor_kind, actor_identity, surface, lane_snapshot, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![uuid::Uuid::new_v4().to_string(), mr_id.as_str(), if matches!(resolution, PrNodeResolution::Merge) { "merge" } else { "close" }, attribution.actor_kind, attribution.actor_identity.as_deref(), attribution.surface, lane_snapshot.as_str(), now],
        ).await.map_err(|e| db_error("Failed to journal PR resolution attribution", e))?;
    }

    if !claimed {
        if !performed {
            return Ok(false);
        }
        // Cairn performed this merge or close, but the row was already resolved:
        // by a refresh that saw this same merge land on GitHub while this call
        // was in flight, or by an earlier resolution. The cascade ran then. What
        // this call adds is who acted and, for a merge, the commit it produced.
        if let (PrNodeResolution::Merge, Some(commit)) = (resolution, merged_commit) {
            db.execute(
                "UPDATE merge_requests SET merged_commit = ?1
                 WHERE id = ?2 AND status = 'merged' AND merged_commit IS NULL",
                params![commit, mr_id.as_str()],
            )
            .await
            .map_err(|e| db_error("Failed to record the merged commit", e))?;
        }
        announce_pr_resolution(orch, &merge_context);
        return Ok(false);
    }

    let issue_to_resolve = match merge_context.issue_id.as_deref() {
        Some(issue_id) if performed => Some(issue_id),
        Some(issue_id) => observation_resolves_issue(&db, issue_id, &mr_id)
            .await?
            .then_some(issue_id),
        None => None,
    };
    // Resolving the PR resolves its issue, and resolving an issue runs the
    // terminal cascade: the issue's live work is stopped, its queued work is
    // cancelled, its sessions close, and any in-flight turn-end review suite is
    // quit (CAIRN-2648, CAIRN-3253).
    if let Some(issue_id) = issue_to_resolve {
        let issue_resolution = match resolution {
            PrNodeResolution::Merge => Resolution::Merged,
            PrNodeResolution::Close => Resolution::Closed,
        };
        resolve_issue_for_pr(orch, &db, issue_id, issue_resolution).await?;
    }

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

    if matches!(resolution, PrNodeResolution::Merge) {
        if let Some(issue_id) = issue_to_resolve.map(str::to_string) {
            let orch_for_memories = orch.clone();
            tokio::spawn(async move {
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

        // Downstream sibling/coordinator reconciliation advances *other* in-flight
        // branches (siblings auto-rebase onto the advanced tip; a Coordinator on
        // the integration branch has its `@` re-parented). It has no bearing on
        // whether THIS PR is merged, is best-effort end to end, and is re-fired
        // idempotently by the GitHub `push` webhook (guarded by the before/after
        // commit-id snapshot in `reconcile_base_advance`). So spawn it off the
        // synchronous merge path: the resolution — and the "merging" button —
        // returns immediately, and the reconcile lands in the background.
        let orch = orch.clone();
        let owner_id = owner_id.to_string();
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            if let Err(e) = crate::orchestrator::base_advance::notify_downstream_of_base_advance(
                &orch, &owner_id,
            )
            .await
            {
                log::warn!("Failed to notify downstream jobs of base advance: {}", e);
            }
            log::info!(
                "Deferred downstream base-advance reconcile for owner {} took {:?}",
                owner_id,
                started.elapsed()
            );
        });
    }

    announce_pr_resolution(orch, &merge_context);
    Ok(true)
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
                "UPDATE action_runs SET status = 'complete', completed_at = ?1 WHERE id = ?2",
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

        // The running builder went through the canonical node stop: its turn is
        // interrupted and its run settled as a stop, not left executing.
        assert_eq!(
            scalar(
                db,
                "SELECT state FROM turns WHERE id = ?1",
                "turn-live-builder"
            )
            .await,
            Some("interrupted".to_string())
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
            Some("user_stop".to_string())
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
                "SELECT state FROM turns WHERE id = ?1",
                "turn-builder-job"
            )
            .await,
            Some("interrupted".to_string()),
            "and nothing it closed is left with a turn in flight"
        );
        assert_eq!(live_run_count(db).await, 0);
    }

    /// The PR door reaches the projection settlement: once the merge lands, no
    /// job on the issue rests running, pending, idle, or blocked — including
    /// the builder the postcondition stopped, which before settlement re-derived
    /// Pending against the merged issue.
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

        let resolved = resolve_observed_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Merge,
            Some("b77e7657"),
        )
        .await
        .expect("the observed merge resolves");
        assert!(resolved, "the first observation resolves the row");

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
            Some("b77e7657"),
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

        assert!(
            resolve_observed_pr_node(&orch, "builder-job", PrNodeResolution::Close, None)
                .await
                .unwrap()
        );

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

        assert!(
            resolve_observed_pr_node(&orch, "builder-job", PrNodeResolution::Close, None)
                .await
                .unwrap()
        );

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

        assert!(
            resolve_observed_pr_node(&orch, "builder-job", PrNodeResolution::Merge, None)
                .await
                .unwrap()
        );
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
        assert!(!stored.contains("a_failing_test"));
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
                .collect(),
        });
        let rendered = render_resolution_provenance(&attribution, &ProvenanceLinks::default());
        assert!(
            rendered.len() < 8_192,
            "{} bytes rendered for 200 lanes",
            rendered.len()
        );
        assert!(
            rendered.contains("136 more lanes are not listed."),
            "{rendered}"
        );
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
