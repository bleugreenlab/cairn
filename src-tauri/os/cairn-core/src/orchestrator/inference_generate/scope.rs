//! What a generating agent's session may do.
//!
//! A generation's agent is an ordinary issue agent, and its tool list is
//! configuration, not enforcement: a model can name any tool and any target.
//! This is the enforcement, asked at the one dispatch door every tool call
//! passes (`crate::dispatch::dispatch_tool`), after a pooled call is resolved
//! to its run and before any handler can produce an effect.
//!
//! Whether a run is scoped is decided only from what this host recorded: the
//! authenticated run, its job and execution, and the private
//! `inference_generations` row its launch wrote. Every job on an issue a
//! generation names is scoped, in every state the generation can be in, so a
//! generation that has not bound its job yet, one that has ended, a resumed
//! conversation, a sub-agent and a second agent on the issue are all held to
//! reads and their own bookkeeping. Only the one job a running generation is
//! bound to (its exact job, node and execution) also reaches that
//! generation's own doors: its draft, its approval and outcome, and
//! verification of its current draft on its pool. A binding proven only by a
//! team replica's synced rows binds nothing, and a lookup that cannot answer
//! refuses.
//!
//! A batch is judged whole: one item outside the scope refuses the call, so a
//! mixed batch has no effects at all.

use std::sync::Arc;

use cairn_uri::CairnResource;
use serde_json::Value;

use super::{get_generation, pool_key, GenerationRow, RUNNING};
use crate::db::DbState;
use crate::mcp::types::{ChangeItem, ChangeMode, ChangePayload, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// The mode of a generation run by an ordinary issue agent. Older rows ran
/// the retired workflow; they still scope their issue, and open no door.
const AGENT_MODE: &str = "agent";

/// The node artifacts a scoped session may write to finish its work.
pub(crate) const COMPLETION_ARTIFACTS: &[&str] = &["return"];

/// The operations a bound session may patch onto its own generation, one per
/// patch.
pub(crate) const GENERATION_OPERATIONS: &[&str] = &["approve", "outcome"];

/// Tools that only read.
const READ_TOOLS: &[&str] = &[
    "read",
    "read_batch",
    "read_web",
    "read_resource",
    "read_issue_resource",
    "watch",
];

/// Keys of a completion artifact patch that act on more than the artifact's
/// own content: confirming a gate, merging or closing a PR, deferring.
const ARTIFACT_ACTIONS: &[&str] = &["confirmed", "action", "defer", "precondition", "method"];

/// The authenticated run, as the scope needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Caller {
    job_id: Option<String>,
    execution_id: Option<String>,
    /// The run's issue and its job's issue, when they have one.
    issue_ids: Vec<String>,
    /// The job's canonical home.
    home: Option<String>,
    /// Whether the run lives in the private database, where this host's own
    /// launch recorded it, rather than in a team replica.
    local: bool,
}

/// A scoped session: the generation that scopes it, and whether it is that
/// generation's bound session.
#[derive(Debug, Clone)]
pub(crate) struct Scope {
    generation: GenerationRow,
    home: Option<String>,
    bound: bool,
}

impl Scope {
    fn refusal(&self, what: &str) -> String {
        let generation = &self.generation;
        let doors = if self.bound {
            format!(
                " As the generation's bound session it may also submit its draft as `generated`, \
                 patch its own generation ({}) with one of {}, and verify its current draft on pool {}.",
                generation.id,
                GENERATION_OPERATIONS.join(", "),
                generation.pool
            )
        } else {
            format!(" {}", self.closed())
        };
        format!(
            "This session generates an inference profile (generation {}, {} on {}) and may not {what}. \
             It may read, wait with a sole `waitFor:{{duration}}` run, ask the operator through its own \
             questions, keep its own todos and completion result, and preview a compose import \
             (`import:{{..., preview:true}}` on cairn://inference/profiles).{doors} Nothing in this call \
             was applied.",
            generation.id, generation.model, generation.pool
        )
    }

    fn closed(&self) -> String {
        if self.generation.state != RUNNING {
            format!(
                "The generation is {}, so its draft, approval, outcome and verification are closed to every session.",
                self.generation.state
            )
        } else {
            "The generation's draft, approval, outcome and verification are open only to the exact job, node \
             and execution its launch bound, and this session is not that one."
                .to_string()
        }
    }

    /// One of the generation's own doors: open only to its bound session.
    fn door(&self, what: &str) -> Result<(), String> {
        if self.bound {
            Ok(())
        } else {
            Err(format!("{what}. {}", self.closed()))
        }
    }
}

/// The refusal for a tool call from a generating session, or `None` when it
/// may proceed, including every call from a run no generation scopes.
pub(crate) async fn refuse(orch: &Orchestrator, request: &McpCallbackRequest) -> Option<String> {
    refuse_in(&orch.db, request).await
}

async fn refuse_in(dbs: &DbState, request: &McpCallbackRequest) -> Option<String> {
    let run_id = request
        .run_id
        .as_deref()
        .filter(|run_id| !run_id.is_empty())?;
    let marker = match marker_for_run(&dbs.local, run_id).await {
        Ok(None) => return None,
        Ok(Some(marker)) => marker,
        Err(error) => {
            log::error!(
                "generation_scope_marker_unreadable: tool={} run={run_id}: {error}",
                request.tool
            );
            return Some(marker_refusal(run_id, &error));
        }
    };
    let scope = match scope_for_marked_run(dbs, run_id, &marker).await {
        Ok(scope) => scope,
        Err(error) => {
            log::error!(
                "generation_scope_lookup_failed: tool={} run={run_id} generation={}: {error}",
                request.tool,
                marker.generation_id
            );
            return Some(lookup_refusal(run_id, &marker.generation_id, &error));
        }
    };
    let refusal = classify(&scope, &request.tool, &request.payload).err()?;
    log::warn!(
        "generation_scope: refused tool={} run={run_id} generation={}: {refusal}",
        request.tool,
        scope.generation.id
    );
    Some(refusal)
}

/// What a run reads when Cairn could not read whether it is a generation's.
fn marker_refusal(run_id: &str, error: &str) -> String {
    format!(
        "This call was not served: Cairn could not read whether run {run_id} belongs to an \
         inference profile generation ({error}). This is a failed lookup, not a scope violation, \
         and nothing in this call was applied. It refuses only this call; send it again."
    )
}

/// What a generation's run reads when its own record could not be read.
fn lookup_refusal(run_id: &str, generation_id: &str, error: &str) -> String {
    format!(
        "This call was not served: run {run_id} belongs to inference profile generation \
         {generation_id}, and Cairn could not read the run or its generation to judge the call \
         ({error}). This is a failed lookup, not a scope violation, and nothing in this call was \
         applied. A transient cause clears on its own and the same call can be sent again."
    )
}

/// A run a generation holds to its scope, as its launch recorded it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Marker {
    generation_id: String,
    job_id: Option<String>,
    issue_id: Option<String>,
}

/// The one read every call pays: whether a generation marked this run.
async fn marker_for_run(private: &LocalDb, run_id: &str) -> Result<Option<Marker>, String> {
    let run_id = run_id.to_string();
    private
        .read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT generation_id, job_id, issue_id FROM inference_generation_runs
                         WHERE run_id = ?1",
                        (run_id.as_str(),),
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                Ok(Some(Marker {
                    generation_id: row.text(0)?,
                    job_id: row.opt_text(1)?.filter(|job| !job.is_empty()),
                    issue_id: row.opt_text(2)?.filter(|issue| !issue.is_empty()),
                }))
            })
        })
        .await
        .map_err(|error| error.to_string())
}

/// The scope of a run its generation marked. The marker alone decides that
/// the run is scoped; the run's own rows decide only whether it is the
/// generation's bound session, and a run whose rows are gone is held to the
/// scope by its marker, bound to nothing.
async fn scope_for_marked_run(
    dbs: &DbState,
    run_id: &str,
    marker: &Marker,
) -> Result<Scope, String> {
    let generation = get_generation(&dbs.local, &marker.generation_id)
        .await?
        .ok_or_else(|| format!("generation {} has no record", marker.generation_id))?;
    let db = crate::execution::routing::routing_db_for_id(dbs, run_id)
        .await
        .map_err(|error| error.to_string())?;
    let local = Arc::ptr_eq(&db, &dbs.local);
    let caller = match caller(&db, run_id, local).await? {
        Some(mut caller) => {
            if let Some(job_id) = &caller.job_id {
                caller.home = crate::jobs::queries::home_uri_for_job(&db, job_id)
                    .await
                    .map_err(|error| error.to_string())?
                    .map(|home| canonical(&home));
            }
            caller
        }
        None => Caller {
            job_id: marker.job_id.clone(),
            execution_id: None,
            issue_ids: marker.issue_id.iter().cloned().collect(),
            home: None,
            local: false,
        },
    };
    Ok(Scope {
        bound: binds(&generation, &caller),
        home: caller.home,
        generation,
    })
}

async fn caller(db: &LocalDb, run_id: &str, local: bool) -> Result<Option<Caller>, String> {
    let run_id = run_id.to_string();
    let row = db
        .read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT r.job_id, r.issue_id, j.issue_id, j.execution_id
                         FROM runs r LEFT JOIN jobs j ON j.id = r.job_id
                         WHERE r.id = ?1 LIMIT 1",
                        (run_id.as_str(),),
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                Ok(Some((
                    row.opt_text(0)?,
                    row.opt_text(1)?,
                    row.opt_text(2)?,
                    row.opt_text(3)?,
                )))
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    let Some((job_id, run_issue, job_issue, execution_id)) = row else {
        return Ok(None);
    };
    let mut issue_ids: Vec<String> = [run_issue, job_issue]
        .into_iter()
        .flatten()
        .filter(|issue| !issue.is_empty())
        .collect();
    issue_ids.dedup();
    Ok(Some(Caller {
        job_id: job_id.filter(|job_id| !job_id.is_empty()),
        execution_id: execution_id.filter(|execution| !execution.is_empty()),
        issue_ids,
        // Read only for a marked run that still has its rows.
        home: None,
        local,
    }))
}

/// Mark a local run for the generation that names its job or one of its
/// issues, if any does, and say which. Called for every run as it is
/// created (first launch, relaunch, resume, sub-agent), before the run can
/// authenticate: an error must fail that launch, since an unmarked run is
/// ordinary. Every generation counts, whatever its state, so a run resumed
/// after its generation ended is held to the scope too. Among several, the
/// generation bound to the run's own job comes first, then the newest. A run
/// already marked keeps its marker.
pub(crate) async fn mark_run(
    private: &LocalDb,
    run_id: &str,
    job_id: Option<&str>,
    issue_ids: &[&str],
) -> Result<Option<String>, String> {
    let run_id = run_id.to_string();
    let job = job_id.unwrap_or_default().to_string();
    let first = issue_ids.first().copied().unwrap_or_default().to_string();
    let second = issue_ids.get(1).copied().unwrap_or_default().to_string();
    private
        .write(move |conn| {
            let (run_id, job, first, second) =
                (run_id.clone(), job.clone(), first.clone(), second.clone());
            Box::pin(async move {
                let mut existing = conn
                    .query(
                        "SELECT generation_id FROM inference_generation_runs WHERE run_id = ?1",
                        (run_id.as_str(),),
                    )
                    .await?;
                if let Some(row) = existing.next().await? {
                    return Ok(Some(row.text(0)?));
                }
                drop(existing);
                let mut named = conn
                    .query(
                        "SELECT id, issue_id FROM inference_generations
                         WHERE (?1 <> '' AND job_id = ?1)
                            OR (?2 <> '' AND issue_id = ?2)
                            OR (?3 <> '' AND issue_id = ?3)
                         ORDER BY (?1 <> '' AND job_id = ?1) DESC, started_at DESC, id DESC
                         LIMIT 1",
                        (job.as_str(), first.as_str(), second.as_str()),
                    )
                    .await?;
                let Some(row) = named.next().await? else {
                    return Ok(None);
                };
                let generation_id = row.text(0)?;
                let issue_id = row
                    .opt_text(1)?
                    .filter(|issue| !issue.is_empty())
                    .or_else(|| Some(first.clone()).filter(|issue| !issue.is_empty()));
                drop(named);
                conn.execute(
                    "INSERT OR IGNORE INTO inference_generation_runs
                       (run_id, generation_id, job_id, issue_id, marked_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    cairn_db::turso::params![
                        run_id.as_str(),
                        generation_id.as_str(),
                        Some(job.as_str()).filter(|job| !job.is_empty()),
                        issue_id.as_deref(),
                        chrono::Utc::now().timestamp(),
                    ],
                )
                .await?;
                Ok(Some(generation_id))
            })
        })
        .await
        .map_err(|error| error.to_string())
}

/// Mark every local run already on a generation's issue, through the run's
/// own issue or its job's, for runs that existed before the generation's
/// row. A run already marked keeps its marker. Returns how many it marked.
pub(crate) async fn mark_issue_runs(
    private: &LocalDb,
    generation_id: &str,
    issue_id: &str,
) -> Result<u64, String> {
    let (generation_id, issue_id) = (generation_id.to_string(), issue_id.to_string());
    private
        .write(move |conn| {
            let (generation_id, issue_id) = (generation_id.clone(), issue_id.clone());
            Box::pin(async move { mark_issue_runs_conn(conn, &generation_id, &issue_id).await })
        })
        .await
        .map_err(|error| error.to_string())
}

/// [`mark_issue_runs`] inside a caller's transaction, so a write that grants
/// a generation authority (its binding) commits with the markers of the runs
/// that authority reaches, or not at all.
pub(crate) async fn mark_issue_runs_conn(
    conn: &cairn_db::storage::TrackedConnection,
    generation_id: &str,
    issue_id: &str,
) -> crate::storage::DbResult<u64> {
    Ok(conn
        .execute(
            "INSERT OR IGNORE INTO inference_generation_runs
           (run_id, generation_id, job_id, issue_id, marked_at)
         SELECT r.id, ?1, r.job_id, ?2, ?3 FROM runs r
         WHERE r.issue_id = ?2
            OR r.job_id IN (SELECT j.id FROM jobs j WHERE j.issue_id = ?2)",
            cairn_db::turso::params![generation_id, issue_id, chrono::Utc::now().timestamp()],
        )
        .await?)
}

/// Whether `caller` is the session `generation`'s launch bound, while the
/// generation runs.
fn binds(generation: &GenerationRow, caller: &Caller) -> bool {
    caller.local
        && generation.mode == AGENT_MODE
        && generation.state == RUNNING
        && !generation.job_id.is_empty()
        && caller.job_id.as_deref() == Some(generation.job_id.as_str())
        && generation.execution_id.is_some()
        && generation.execution_id == caller.execution_id
        && !generation.node_uri.is_empty()
        && caller.home.as_deref() == Some(canonical(&generation.node_uri).as_str())
        && generation
            .issue_id
            .as_ref()
            .is_some_and(|issue| caller.issue_ids.contains(issue))
}

fn canonical(uri: &str) -> String {
    cairn_uri::parse_uri(uri)
        .map(|resource| resource.to_uri())
        .unwrap_or_else(|| uri.to_string())
}

/// Whether a scoped session may make the tool call `tool` with `payload`.
pub(crate) fn classify(scope: &Scope, tool: &str, payload: &Value) -> Result<(), String> {
    match tool {
        tool if READ_TOOLS.contains(&tool) => Ok(()),
        "run" => classify_run(payload).map_err(|what| scope.refusal(&what)),
        "write" => classify_write(scope, payload).map_err(|what| scope.refusal(&what)),
        other => Err(scope.refusal(&format!("use `{other}`"))),
    }
}

/// A `run` batch may only wait: one `waitFor:{duration}` item, nothing else.
fn classify_run(payload: &Value) -> Result<(), String> {
    const ONLY: &str = "run anything but a sole `waitFor:{duration}` item";
    let Some(batch) = payload.as_object() else {
        return Err(ONLY.to_string());
    };
    if let Some((key, _)) = batch
        .iter()
        .find(|(key, value)| key.as_str() != "commands" && !value.is_null())
    {
        return Err(format!("run a batch with `{key}`"));
    }
    let Some([item]) = batch
        .get("commands")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
    else {
        return Err(ONLY.to_string());
    };
    let Some(item) = item.as_object() else {
        return Err(ONLY.to_string());
    };
    if let Some((key, _)) = item
        .iter()
        .find(|(key, value)| !value.is_null() && !matches!(key.as_str(), "waitFor" | "description"))
    {
        return Err(format!("run an item with `{key}`"));
    }
    match item.get("waitFor").and_then(Value::as_object) {
        Some(wait)
            if wait.len() == 1
                && wait
                    .get("duration")
                    .is_some_and(|duration| duration.is_string() || duration.is_u64()) =>
        {
            Ok(())
        }
        _ => Err(ONLY.to_string()),
    }
}

/// A `write` batch is read exactly as the write handler reads it, and every
/// item must be in scope.
fn classify_write(scope: &Scope, payload: &Value) -> Result<(), String> {
    let batch: ChangePayload = serde_json::from_value(payload.clone())
        .map_err(|error| format!("write a batch Cairn cannot read ({error})"))?;
    if batch.commit_msg.is_some() || batch.conflict_markers_reason.is_some() {
        return Err("commit (`commit_msg`): a generating session edits no repository".to_string());
    }
    if batch.changes.is_empty() {
        return Err("write without naming a target".to_string());
    }
    for item in &batch.changes {
        classify_change(scope, item)?;
    }
    Ok(())
}

fn mode_of(mode: ChangeMode) -> String {
    serde_json::to_value(mode)
        .ok()
        .and_then(|mode| mode.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{mode:?}"))
}

fn classify_change(scope: &Scope, item: &ChangeItem) -> Result<(), String> {
    let what = || format!("`{}` `{}`", mode_of(item.mode), item.target);
    if item.mode == ChangeMode::Apply {
        return Err(
            "apply a previewed batch (`mode:\"apply\"`): send the previewed changes directly instead"
                .to_string(),
        );
    }
    let target = item.target.trim();
    if !target.starts_with("cairn:") {
        return Err(format!("{}: a generating session edits no files", what()));
    }
    if target.contains(['?', '#']) {
        return Err(format!("{}: a write target carries no query", what()));
    }
    let resolved = scope
        .home
        .as_deref()
        .and_then(|home| crate::resources::resolve_home_relative_against(home, target))
        .unwrap_or_else(|| target.to_string());
    let Some(resource) = cairn_uri::parse_uri(&resolved) else {
        return Err(format!("{}: it names nothing this scope admits", what()));
    };
    if let Some(home) = scope.home.as_deref() {
        let written = resource.to_uri();
        let own = |suffix: &str| written == canonical(&format!("{home}/{suffix}"));
        if own("questions") {
            return match item.mode {
                ChangeMode::Append => Ok(()),
                _ => Err(format!("{}: its own questions are only asked", what())),
            };
        }
        if own("todos") {
            return match item.mode {
                ChangeMode::Create
                | ChangeMode::Append
                | ChangeMode::Patch
                | ChangeMode::Replace
                | ChangeMode::Delete => Ok(()),
                _ => Err(what()),
            };
        }
        if COMPLETION_ARTIFACTS.iter().any(|artifact| own(artifact)) {
            return completion(item).map_err(|why| format!("{}: {why}", what()));
        }
    }
    match (&resource, item.mode) {
        (CairnResource::InferenceProfiles, ChangeMode::Append) => match sole_key(item) {
            Some(("generated", _)) => scope.door("submit a draft as `generated`"),
            Some(("import", spec)) if spec.get("preview") == Some(&Value::Bool(true)) => Ok(()),
            _ => Err(format!(
                "{}: the profiles door admits only `{{import:{{..., preview:true}}}}` or the generation's own \
                 `{{generated}}` draft, one per item",
                what()
            )),
        },
        (CairnResource::InferenceGeneration { id }, ChangeMode::Patch) => {
            if *id != scope.generation.id {
                return Err(format!("{}: it is not this session's generation", what()));
            }
            match sole_key(item) {
                Some((operation, _)) if GENERATION_OPERATIONS.contains(&operation) => {
                    scope.door(&format!("`{operation}` generation {id}"))
                }
                _ => Err(format!(
                    "{}: a generation patch carries exactly one of {}",
                    what(),
                    GENERATION_OPERATIONS.join(", ")
                )),
            }
        }
        (CairnResource::InferenceProfile { id }, ChangeMode::Patch) => {
            let pool = match sole_key(item) {
                Some(("verify", Value::Object(verify))) if verify.len() == 1 => {
                    verify.get("pool").and_then(Value::as_str)
                }
                _ => None,
            };
            let Some(pool) = pool else {
                return Err(format!(
                    "{}: a profile patch here is exactly `{{verify:{{pool}}}}`",
                    what()
                ));
            };
            if scope.generation.draft_profile.as_deref() != Some(id.as_str()) {
                return Err(format!(
                    "{}: only the generation's current draft is verified here",
                    what()
                ));
            }
            if pool_key(pool) != pool_key(&scope.generation.pool) {
                return Err(format!(
                    "{}: the draft is verified only on the generation's pool, {}",
                    what(),
                    scope.generation.pool
                ));
            }
            scope.door(&format!("verify profile {id}"))
        }
        _ => Err(what()),
    }
}

/// A completion artifact's own content, never an action on what it gates.
fn completion(item: &ChangeItem) -> Result<(), String> {
    if !matches!(item.mode, ChangeMode::Create | ChangeMode::Patch) {
        return Err("a completion result is created or patched".to_string());
    }
    let acts = item
        .payload
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|payload| {
            payload
                .keys()
                .find(|key| ARTIFACT_ACTIONS.contains(&key.as_str()))
        });
    match acts {
        Some(key) => Err(format!("`{key}` acts on more than the result's content")),
        None => Ok(()),
    }
}

/// An item's payload when it is an object of exactly one key. A second key,
/// even a null one, is refused: the door reads keys by presence.
fn sole_key(item: &ChangeItem) -> Option<(&str, &Value)> {
    let payload = item.payload.as_ref()?.as_object()?;
    if payload.len() != 1 {
        return None;
    }
    payload
        .iter()
        .next()
        .map(|(key, value)| (key.as_str(), value))
}

/// Whether a change submits a generation's draft, which spends one of its
/// bounded attempts.
pub(crate) fn submits_draft(item: &ChangeItem) -> bool {
    let target = item.target.split(['?', '#']).next().unwrap_or(&item.target);
    item.mode == ChangeMode::Append
        && matches!(
            cairn_uri::parse_uri(target),
            Some(CairnResource::InferenceProfiles)
        )
        && item
            .payload
            .as_ref()
            .is_some_and(|payload| payload.get("generated").is_some())
}

/// Why a draft is refused when the write replay ledger cannot hold its call.
pub(crate) const UNGUARDED_DRAFT: &str = "A generated draft spends one of its generation's bounded \
     attempts, so it is applied only when this write can be tied to its own `write` call and recorded \
     against a second delivery. This delivery could not be, so nothing was applied and no attempt was \
     spent. Send the `generated` item alone, in a write of its own, and send it again if this persists.";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SearchIndex;
    use serde_json::json;

    const HOME: &str = "cairn://p/prj/9/1/agent";

    fn generation(state: &str) -> GenerationRow {
        GenerationRow {
            id: "g-1".into(),
            project_id: "p".into(),
            issue_id: Some("i9".into()),
            job_id: "j-agent".into(),
            mode: AGENT_MODE.into(),
            execution_id: Some("e1".into()),
            attempts: 0,
            draft_revision: 1,
            draft_digest: None,
            approval_prompt_id: None,
            approval_run_id: None,
            approval_revision: None,
            verification_id: None,
            cleanup_pending: false,
            node_uri: HOME.into(),
            model: "Qwen/Qwen3-Embedding-4B".into(),
            pool: "bbs-1+bbs-2".into(),
            state: state.into(),
            draft_profile: Some("qwen-draft".into()),
            draft_written_at: Some(1),
            approved_surface: None,
            hardware: Default::default(),
            outcome: None,
            started_at: 1,
            finished_at: None,
        }
    }

    fn scope(bound: bool) -> Scope {
        Scope {
            generation: generation(RUNNING),
            home: Some(HOME.into()),
            bound,
        }
    }

    fn write(changes: Value) -> Value {
        json!({ "changes": changes })
    }

    fn one(target: &str, mode: &str, payload: Value) -> Value {
        write(json!([{ "target": target, "mode": mode, "payload": payload }]))
    }

    fn admits(scope: &Scope, tool: &str, payload: Value) -> bool {
        classify(scope, tool, &payload).is_ok()
    }

    /// Reads and the sole duration wait run for every scoped session; no other
    /// tool and no other kind of run item does.
    #[test]
    fn a_scoped_session_reads_and_waits_but_runs_nothing() {
        let scope = scope(false);
        for tool in READ_TOOLS {
            assert!(
                admits(&scope, tool, json!({"paths": ["file:src/lib.rs"]})),
                "{tool}"
            );
        }
        for tool in ["process_info", "check_run", "change", "unknown"] {
            assert!(!admits(&scope, tool, json!({})), "{tool}");
        }
        assert!(admits(
            &scope,
            "run",
            json!({"commands": [{"waitFor": {"duration": "3m"}}]})
        ));
        assert!(admits(
            &scope,
            "run",
            json!({"commands": [{"waitFor": {"duration": 1000}, "description": "wait"}]})
        ));
        for refused in [
            json!({"commands": [{"command": "ls"}]}),
            json!({"commands": [{"code": "1", "interpreter": "python"}]}),
            json!({"commands": [{"target": "cairn://mcp/turso/query"}]}),
            json!({"commands": [{"target": "cairn://skills/testing/scripts/run"}]}),
            json!({"commands": [{"code": "1", "interpreter": "python", "repl": "x"}]}),
            json!({"commands": [{"waitFor": {"kind": "terminal", "ref": "cairn:~/terminal/x", "on": "exit"}}]}),
            json!({"commands": [{"waitFor": {"duration": "3m"}, "command": "ls"}]}),
            json!({"commands": [{"waitFor": {"duration": "3m"}}, {"waitFor": {"duration": "3m"}}]}),
            json!({"commands": [{"waitFor": {"duration": "3m"}}], "commit_msg": "x"}),
            json!({"commands": [{"waitFor": {"duration": "3m"}}], "executor": {"os": "linux"}}),
            json!({"commands": []}),
        ] {
            assert!(!admits(&scope, "run", refused.clone()), "{refused}");
        }
    }

    /// Its own questions, todos and completion result are reachable under
    /// every spelling of its home, and no one else's are.
    #[test]
    fn a_scoped_session_keeps_only_its_own_bookkeeping_under_every_alias() {
        let scope = scope(false);
        for target in [
            "cairn:~/questions",
            "cairn://p/prj/9/1/agent/questions",
            "cairn://p/PRJ/9/1/agent/questions",
            "cairn:@/9/1/agent/questions",
        ] {
            assert!(
                admits(
                    &scope,
                    "write",
                    one(target, "append", json!({"questions": []}))
                ),
                "{target}"
            );
        }
        assert!(!admits(
            &scope,
            "write",
            one("cairn:~/questions", "patch", json!({}))
        ));
        for mode in ["create", "append", "patch", "replace", "delete"] {
            assert!(
                admits(&scope, "write", one("cairn:~/todos", mode, json!({}))),
                "{mode}"
            );
        }
        assert!(admits(
            &scope,
            "write",
            one("cairn:~/return", "create", json!({"content": "done"}))
        ));
        assert!(admits(
            &scope,
            "write",
            one("cairn:~/return", "patch", json!({"content": "done"}))
        ));
        for refused in [
            one("cairn:~/return", "patch", json!({"confirmed": true})),
            one("cairn:~/return", "patch", json!({"action": "merge"})),
            one(
                "cairn:~/plan",
                "create",
                json!({"title": "t", "content": "c"}),
            ),
            one(
                "cairn:~/create-pr",
                "create",
                json!({"title": "t", "body": "b"}),
            ),
            one(
                "cairn://p/prj/9/1/other/questions",
                "append",
                json!({"questions": []}),
            ),
            one("cairn://p/prj/8/1/agent/todos", "patch", json!({})),
            one("cairn:~/tasks", "append", json!({"prompt": "x"})),
            one("cairn:~/permissions", "append", json!({})),
            one("cairn:~/terminal/dev", "create", json!({"command": "bash"})),
            one("cairn:~/browser", "create", json!({"url": "https://x"})),
            one(
                "cairn:~/repl/py",
                "create",
                json!({"interpreter": "python"}),
            ),
            one("cairn:~/memories", "append", json!({"content": "x"})),
            one("cairn:~/wakes", "append", json!({})),
            one("cairn:~/questions?x=1", "append", json!({"questions": []})),
            one(
                "cairn://p/prj/9/messages",
                "append",
                json!({"content": "x"}),
            ),
            one("cairn://p/prj/12", "patch", json!({"status": "closed"})),
            one("cairn://p/prj/issues", "append", json!({"title": "x"})),
            one("cairn://posts", "append", json!({"content": "x"})),
        ] {
            assert!(!admits(&scope, "write", refused.clone()), "{refused}");
        }
    }

    /// No file target is reachable in any mode, nor a commit, nor apply.
    #[test]
    fn a_scoped_session_edits_no_file_commits_nothing_and_applies_no_preview() {
        let scope = scope(true);
        for mode in [
            "create",
            "append",
            "patch",
            "replace",
            "delete",
            "unified_patch",
            "rename",
        ] {
            for target in ["file:src/lib.rs", "src/lib.rs", "file:", "file:/etc/hosts"] {
                assert!(
                    !admits(&scope, "write", one(target, mode, json!({"content": "x"}))),
                    "{mode} {target}"
                );
            }
        }
        assert!(!admits(
            &scope,
            "write",
            one("file:", "revert", json!({"commit": "abc"}))
        ));
        assert!(!admits(
            &scope,
            "write",
            json!({"changes": [{"target": "cairn://p/prj/9/1/agent/chat/1/7", "mode": "apply"}], "commit_msg": "^"})
        ));
        assert!(!admits(
            &scope,
            "write",
            json!({"changes": [{"target": "cairn://p/prj/9/1/agent/chat/1/7", "mode": "apply"}]})
        ));
        assert!(!admits(
            &scope,
            "write",
            json!({"changes": [{"target": "cairn:~/todos", "mode": "patch", "payload": {}}], "commit_msg": "x"})
        ));
        assert!(!admits(&scope, "write", write(json!([]))));
        // A preview of an admitted item writes nothing and is admitted; a
        // preview of a refused one is refused all the same.
        assert!(admits(
            &scope,
            "write",
            json!({"preview": true, "changes": [{"target": "cairn://inference/profiles", "mode": "append", "payload": {"generated": {}}}]})
        ));
        assert!(!admits(
            &scope,
            "write",
            json!({"preview": true, "changes": [{"target": "file:a", "mode": "create", "payload": {"content": "x"}}]})
        ));
    }

    /// One refused item refuses the whole batch, before any item applies.
    #[test]
    fn a_mixed_batch_is_refused_whole() {
        let scope = scope(true);
        let questions =
            json!({"target": "cairn:~/questions", "mode": "append", "payload": {"questions": []}});
        let draft = json!({"target": "cairn://inference/profiles", "mode": "append", "payload": {"generated": {}}});
        assert!(admits(
            &scope,
            "write",
            write(json!([questions.clone(), draft.clone()]))
        ));
        for intruder in [
            json!({"target": "file:src/lib.rs", "mode": "create", "payload": {"content": "x"}}),
            json!({"target": "cairn:~/tasks", "mode": "append", "payload": {"prompt": "x"}}),
            json!({"target": "cairn://inference/pools", "mode": "patch", "payload": {"pool": "bbs"}}),
        ] {
            for order in [
                json!([questions.clone(), draft.clone(), intruder.clone()]),
                json!([intruder.clone(), questions.clone()]),
            ] {
                assert!(!admits(&scope, "write", write(order.clone())), "{order}");
            }
        }
        // Two operations in one payload are refused as well.
        assert!(!admits(
            &scope,
            "write",
            one(
                "cairn://inference/profiles",
                "append",
                json!({"generated": {}, "import": {"preview": true}})
            )
        ));
        assert!(!admits(
            &scope,
            "write",
            one(
                "cairn://inference/profiles",
                "append",
                json!({"generated": {}, "generate": null})
            )
        ));
        assert!(!admits(
            &scope,
            "write",
            one(
                "cairn://inference/generations/g-1",
                "patch",
                json!({"approve": {}, "outcome": {}})
            )
        ));
    }

    /// The generation's doors open to its bound session alone, and only for
    /// this generation, its current draft and its pool.
    #[test]
    fn the_generations_own_doors_open_only_to_its_bound_session() {
        let bound = scope(true);
        let other = scope(false);
        let draft = one(
            "cairn://inference/profiles",
            "append",
            json!({"generated": {"profile": "x"}}),
        );
        let approve = one(
            "cairn://inference/generations/g-1",
            "patch",
            json!({"approve": {}}),
        );
        let raise = one(
            "cairn://inference/generations/g-1",
            "patch",
            json!({"requestApproval": true}),
        );
        let outcome = one(
            "cairn://inference/generations/g-1",
            "patch",
            json!({"outcome": {"outcome": "gaveUp"}}),
        );
        let verify = one(
            "cairn://inference/profiles/qwen-draft",
            "patch",
            json!({"verify": {"pool": "bbs-1+bbs-2"}}),
        );
        let verify_alias = one(
            "cairn://inference/profiles/qwen-draft",
            "patch",
            json!({"verify": {"pool": "bbs-2+bbs-1"}}),
        );
        for door in [&draft, &approve, &outcome, &verify, &verify_alias] {
            assert!(admits(&bound, "write", door.clone()), "{door}");
            let refusal = classify(&other, "write", door).unwrap_err();
            assert!(refusal.contains("exact job, node"), "{refusal}");
        }
        // Approval is raised by the server, never by an operation the agent names.
        assert!(classify(&bound, "write", &raise).is_err());
        assert!(classify(&other, "write", &raise).is_err());
        let preview = one(
            "cairn://inference/profiles",
            "append",
            json!({"import": {"url": "u", "service": "s", "preview": true}}),
        );
        assert!(admits(&other, "write", preview.clone()));
        assert!(admits(&bound, "write", preview));
        for refused in [
            one(
                "cairn://inference/profiles",
                "append",
                json!({"import": {"url": "u", "service": "s"}}),
            ),
            one(
                "cairn://inference/profiles",
                "append",
                json!({"import": {"url": "u", "preview": "true"}}),
            ),
            one(
                "cairn://inference/profiles",
                "append",
                json!({"import": {"url": "u", "preview": false}}),
            ),
            one(
                "cairn://inference/profiles",
                "append",
                json!({"generate": {}}),
            ),
            one(
                "cairn://inference/generations/g-2",
                "patch",
                json!({"approve": {}}),
            ),
            one(
                "cairn://inference/generations/g-1",
                "patch",
                json!({"cancel": true}),
            ),
            one(
                "cairn://inference/profiles/other",
                "patch",
                json!({"verify": {"pool": "bbs-1+bbs-2"}}),
            ),
            one(
                "cairn://inference/profiles/qwen-draft",
                "patch",
                json!({"verify": {"pool": "halo"}}),
            ),
            one(
                "cairn://inference/profiles/qwen-draft",
                "patch",
                json!({"verify": {"pool": "bbs-1+bbs-2", "all": true}}),
            ),
            one(
                "cairn://inference/models/Qwen%2FQwen3-Embedding-4B",
                "patch",
                json!({"verify": {"pool": "bbs-1+bbs-2"}}),
            ),
            one("cairn://inference/models", "append", json!({"repo": "x/y"})),
            one("cairn://inference", "patch", json!({"upsert": {}})),
            one("cairn://inference", "replace", json!({"yaml": ""})),
            one(
                "cairn://inference/pools",
                "patch",
                json!({"pool": "bbs", "pin": "x"}),
            ),
            one(
                "cairn://inference/requests",
                "append",
                json!({"model": "x"}),
            ),
            one(
                "cairn://inference/deployments/d",
                "patch",
                json!({"action": "start"}),
            ),
            one("cairn://settings", "patch", json!({})),
        ] {
            assert!(!admits(&bound, "write", refused.clone()), "{refused}");
        }
        // A generation with no current draft verifies nothing.
        let mut drafted = scope(true);
        drafted.generation.draft_profile = None;
        assert!(!admits(&drafted, "write", verify));
    }

    fn caller() -> Caller {
        Caller {
            job_id: Some("j-agent".into()),
            execution_id: Some("e1".into()),
            issue_ids: vec!["i9".into()],
            home: Some(HOME.into()),
            local: true,
        }
    }

    /// The binding is exact: job, node, execution, issue, a running agent
    /// generation, and a run this host's private database holds.
    #[test]
    fn only_the_exact_local_job_node_and_execution_bind() {
        assert!(binds(&generation(RUNNING), &caller()));
        let mut uppercase = generation(RUNNING);
        uppercase.node_uri = "cairn://p/PRJ/9/1/agent".into();
        assert!(
            binds(&uppercase, &caller()),
            "a node is matched canonically"
        );
        /// One way a generation or its caller can differ from the binding.
        type Unbind = Box<dyn Fn(&mut GenerationRow, &mut Caller)>;
        let unbinding: Vec<(&str, Unbind)> = vec![
            (
                "another job",
                Box::new(|_, c| c.job_id = Some("j-other".into())),
            ),
            (
                "another node",
                Box::new(|_, c| c.home = Some("cairn://p/prj/9/1/other".into())),
            ),
            (
                "a sub-agent",
                Box::new(|_, c| c.home = Some(format!("{HOME}/task/probe"))),
            ),
            (
                "another execution",
                Box::new(|_, c| c.execution_id = Some("e2".into())),
            ),
            (
                "no execution",
                Box::new(|g, c| {
                    g.execution_id = None;
                    c.execution_id = None;
                }),
            ),
            (
                "another issue",
                Box::new(|_, c| c.issue_ids = vec!["i8".into()]),
            ),
            ("a team replica's run", Box::new(|_, c| c.local = false)),
            (
                "a starting generation",
                Box::new(|g, _| g.state = "starting".into()),
            ),
            (
                "an ended generation",
                Box::new(|g, _| g.state = "gaveUp".into()),
            ),
            (
                "a legacy workflow",
                Box::new(|g, _| g.mode = "workflow".into()),
            ),
            (
                "an unbound job",
                Box::new(|g, c| {
                    g.job_id = String::new();
                    c.job_id = None;
                }),
            ),
            (
                "an unbound node",
                Box::new(|g, _| g.node_uri = String::new()),
            ),
        ];
        for (case, change) in unbinding {
            let (mut generation, mut caller) = (generation(RUNNING), caller());
            change(&mut generation, &mut caller);
            assert!(!binds(&generation, &caller), "{case}");
        }
    }

    #[test]
    fn a_generated_draft_is_recognised_wherever_the_replay_guard_must_hold() {
        let item = |target: &str, mode: ChangeMode, payload: Value| ChangeItem {
            target: target.into(),
            mode,
            payload: Some(payload),
        };
        assert!(submits_draft(&item(
            "cairn://inference/profiles",
            ChangeMode::Append,
            json!({"generated": {}})
        )));
        assert!(!submits_draft(&item(
            "cairn://inference/profiles",
            ChangeMode::Append,
            json!({"import": {"preview": true}})
        )));
        assert!(!submits_draft(&item(
            "cairn:~/questions",
            ChangeMode::Append,
            json!({"generated": {}})
        )));
        assert!(!submits_draft(&item(
            "cairn://inference/profiles",
            ChangeMode::Patch,
            json!({"generated": {}})
        )));
    }

    // ---- resolution from the databases -------------------------------------

    async fn dbs() -> DbState {
        let dir = tempfile::tempdir().unwrap().keep();
        let db = LocalDb::open_migrated_for_test(dir.join("scope.turso.db"))
            .await
            .unwrap();
        let search = SearchIndex::open_or_create(dir.join("scope.search")).unwrap();
        let dbs = DbState::new(Arc::new(db), Arc::new(search));
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i9','p',9,'Generate profile','active',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i8','p',8,'Other','active',1,1)",
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e1','inference-generate','i9','p','running',1,1)",
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e8','recipe','i8','p','running',1,1)",
            "INSERT INTO jobs (id, issue_id, project_id, execution_id, status, node_name, uri_segment, created_at, updated_at) VALUES ('j-agent','i9','p','e1','running','agent','agent',1,1)",
            "INSERT INTO jobs (id, issue_id, project_id, execution_id, status, node_name, uri_segment, created_at, updated_at) VALUES ('j-second','i9','p','e1','running','second','second',1,1)",
            "INSERT INTO jobs (id, issue_id, project_id, execution_id, status, node_name, uri_segment, created_at, updated_at) VALUES ('j-builder','i8','p','e8','running','builder','builder',1,1)",
            "INSERT INTO runs (id, project_id, job_id, issue_id, status, created_at, updated_at) VALUES ('r-agent','p','j-agent','i9','live',1,1)",
            "INSERT INTO runs (id, project_id, job_id, issue_id, status, created_at, updated_at) VALUES ('r-resumed','p','j-agent','i9','live',2,2)",
            "INSERT INTO runs (id, project_id, job_id, issue_id, status, created_at, updated_at) VALUES ('r-second','p','j-second','i9','live',1,1)",
            "INSERT INTO runs (id, project_id, job_id, issue_id, status, created_at, updated_at) VALUES ('r-builder','p','j-builder','i8','live',1,1)",
        ] {
            dbs.local.execute(sql, ()).await.unwrap();
        }
        dbs
    }

    fn request(run_id: &str, tool: &str, payload: Value) -> McpCallbackRequest {
        serde_json::from_value(json!({
            "tool": tool,
            "payload": payload,
            "cwd": "/tmp",
            "run_id": run_id,
        }))
        .unwrap()
    }

    /// Record a generation the way its launch does: its row, then a marker on
    /// every run already on its issue.
    async fn with_generation(dbs: &DbState, row: GenerationRow) {
        super::super::insert_generation(&dbs.local, &row)
            .await
            .unwrap();
        mark_issue_runs(&dbs.local, &row.id, row.issue_id.as_deref().unwrap())
            .await
            .unwrap();
    }

    async fn marker(dbs: &DbState, run_id: &str) -> Option<Marker> {
        marker_for_run(&dbs.local, run_id).await.unwrap()
    }

    fn draft() -> Value {
        one(
            "cairn://inference/profiles",
            "append",
            json!({"generated": {"profile": "x"}}),
        )
    }

    fn file_edit() -> Value {
        json!({"changes": [{"target": "file:src/lib.rs", "mode": "create", "payload": {"content": "x"}}], "commit_msg": "x"})
    }

    #[tokio::test]
    async fn the_bound_run_reaches_its_doors_and_every_other_issue_job_is_held_to_reads() {
        let dbs = dbs().await;
        assert_eq!(
            crate::jobs::queries::home_uri_for_job(&dbs.local, "j-agent")
                .await
                .unwrap()
                .as_deref(),
            Some(HOME)
        );
        // Before any generation exists, nothing is scoped.
        assert!(refuse_in(&dbs, &request("r-agent", "write", file_edit()))
            .await
            .is_none());
        with_generation(&dbs, generation(RUNNING)).await;

        assert!(refuse_in(&dbs, &request("r-agent", "write", draft()))
            .await
            .is_none());
        // A resumed session of the same bound job is the same session.
        assert!(refuse_in(&dbs, &request("r-resumed", "write", draft()))
            .await
            .is_none());
        assert!(refuse_in(&dbs, &request("r-agent", "write", file_edit()))
            .await
            .is_some());
        assert!(refuse_in(
            &dbs,
            &request(
                "r-agent",
                "run",
                json!({"commands": [{"command": "rm -rf /"}]})
            )
        )
        .await
        .is_some());

        // Another job on the generation's issue reads, and reaches no door.
        let second = refuse_in(&dbs, &request("r-second", "write", draft()))
            .await
            .unwrap();
        assert!(second.contains("exact job, node"), "{second}");
        assert!(refuse_in(
            &dbs,
            &request("r-second", "read_batch", json!({"paths": ["cairn:~"]}))
        )
        .await
        .is_none());

        // An unrelated issue's builder is untouched.
        assert!(refuse_in(&dbs, &request("r-builder", "write", file_edit()))
            .await
            .is_none());
        // A run no generation marked is ordinary, record or not: this check
        // leaves a missing run to the fences that own that question.
        assert!(refuse_in(&dbs, &request("r-missing", "write", file_edit()))
            .await
            .is_none());
        // Nor is a request with no run.
        assert!(refuse_in(&dbs, &request("", "write", file_edit()))
            .await
            .is_none());
    }

    #[tokio::test]
    async fn a_starting_generation_scopes_every_job_on_its_issue_and_opens_no_door() {
        let dbs = dbs().await;
        let mut starting = generation("starting");
        starting.job_id = String::new();
        starting.node_uri = String::new();
        starting.execution_id = None;
        with_generation(&dbs, starting).await;
        for run in ["r-agent", "r-second"] {
            assert!(
                refuse_in(&dbs, &request(run, "write", draft()))
                    .await
                    .is_some(),
                "{run}"
            );
            assert!(
                refuse_in(&dbs, &request(run, "write", file_edit()))
                    .await
                    .is_some(),
                "{run}"
            );
            assert!(
                refuse_in(&dbs, &request(run, "read", json!({"path": "x"})))
                    .await
                    .is_none(),
                "{run}"
            );
        }
        assert!(refuse_in(&dbs, &request("r-builder", "write", file_edit()))
            .await
            .is_none());
    }

    #[tokio::test]
    async fn an_ended_generation_keeps_its_session_scoped_with_its_doors_closed() {
        let dbs = dbs().await;
        let mut ended = generation("gaveUp");
        ended.finished_at = Some(2);
        with_generation(&dbs, ended).await;
        for run in ["r-agent", "r-resumed"] {
            let refusal = refuse_in(&dbs, &request(run, "write", draft()))
                .await
                .unwrap();
            assert!(refusal.contains("is gaveUp"), "{refusal}");
            for door in [
                one(
                    "cairn://inference/generations/g-1",
                    "patch",
                    json!({"approve": {}}),
                ),
                one(
                    "cairn://inference/profiles/qwen-draft",
                    "patch",
                    json!({"verify": {"pool": "bbs-1+bbs-2"}}),
                ),
            ] {
                assert!(
                    refuse_in(&dbs, &request(run, "write", door))
                        .await
                        .is_some(),
                    "{run}"
                );
            }
            assert!(
                refuse_in(&dbs, &request(run, "write", file_edit()))
                    .await
                    .is_some(),
                "{run}"
            );
            // It can still talk: ask, and keep its own todos.
            assert!(refuse_in(
                &dbs,
                &request(
                    run,
                    "write",
                    one("cairn:~/questions", "append", json!({"questions": []}))
                )
            )
            .await
            .is_none());
        }
    }

    #[tokio::test]
    async fn a_generation_bound_to_another_execution_or_node_opens_no_door() {
        for change in [
            (|g: &mut GenerationRow| g.execution_id = Some("e-other".into()))
                as fn(&mut GenerationRow),
            |g: &mut GenerationRow| g.node_uri = "cairn://p/prj/9/1/second".into(),
            |g: &mut GenerationRow| g.job_id = "j-second".into(),
        ] {
            let dbs = dbs().await;
            let mut row = generation(RUNNING);
            change(&mut row);
            with_generation(&dbs, row).await;
            assert!(refuse_in(&dbs, &request("r-agent", "write", draft()))
                .await
                .is_some());
        }
    }

    const TEAM_RUN: &str = "team-1~00000000-0000-4000-8000-000000000001";

    /// An unmarked run is answered from its absent marker alone: neither its
    /// missing row nor its closed team replica is read, so neither refuses.
    #[tokio::test]
    async fn an_unmarked_run_is_ordinary_without_reading_its_run_or_route() {
        let dbs = dbs().await;
        with_generation(&dbs, generation(RUNNING)).await;
        for run in ["r-missing", TEAM_RUN, "r-builder"] {
            assert_eq!(marker(&dbs, run).await, None, "{run}");
            assert!(
                refuse_in(&dbs, &request(run, "write", file_edit()))
                    .await
                    .is_none(),
                "{run}"
            );
        }
        // The same closed replica behind a marked run refuses that call, as a
        // failed lookup rather than a scope violation.
        assert_eq!(
            mark_run(&dbs.local, TEAM_RUN, Some("j-agent"), &["i9"])
                .await
                .unwrap()
                .as_deref(),
            Some("g-1")
        );
        let refusal = refuse_in(&dbs, &request(TEAM_RUN, "read", json!({"path": "x"})))
            .await
            .unwrap();
        assert!(refusal.contains("failed lookup"), "{refusal}");
        assert!(refusal.contains("generation g-1"), "{refusal}");
        assert!(!refusal.contains("may not"), "{refusal}");
    }

    /// Erasing a bound job erases its runs and its home with it, but not its
    /// runs' markers: the still-live process stays scoped, bound to nothing,
    /// so it reads and every mutation and door is refused.
    #[tokio::test]
    async fn an_erased_bound_job_leaves_its_runs_scoped_by_their_markers() {
        let dbs = dbs().await;
        with_generation(&dbs, generation(RUNNING)).await;
        dbs.local
            .execute("DELETE FROM jobs WHERE id = 'j-agent'", ())
            .await
            .unwrap();
        dbs.local
            .execute("DELETE FROM issues WHERE id = 'i9'", ())
            .await
            .unwrap();
        for run in ["r-agent", "r-resumed"] {
            assert!(marker(&dbs, run).await.is_some(), "{run}");
            let door = refuse_in(&dbs, &request(run, "write", draft()))
                .await
                .unwrap();
            assert!(door.contains("exact job, node"), "{run}: {door}");
            for (tool, payload) in [
                ("write", file_edit()),
                ("run", json!({"commands": [{"command": "true"}]})),
                (
                    "write",
                    one("cairn:~/todos", "replace", json!({"todos": []})),
                ),
            ] {
                assert!(
                    refuse_in(&dbs, &request(run, tool, payload))
                        .await
                        .is_some(),
                    "{run} {tool}"
                );
            }
            assert!(
                refuse_in(&dbs, &request(run, "read", json!({"path": "x"})))
                    .await
                    .is_none(),
                "{run}"
            );
        }
    }

    /// A marker whose generation has no row refuses, never releases.
    #[tokio::test]
    async fn a_marker_naming_a_missing_generation_refuses() {
        let dbs = dbs().await;
        dbs.local
            .execute(
                "INSERT INTO inference_generation_runs (run_id, generation_id, job_id, issue_id, marked_at)
                 VALUES ('r-agent', 'g-gone', 'j-agent', 'i9', 1)",
                (),
            )
            .await
            .unwrap();
        let refusal = refuse_in(&dbs, &request("r-agent", "read", json!({"path": "x"})))
            .await
            .unwrap();
        assert!(refusal.contains("generation g-gone"), "{refusal}");
        assert!(refusal.contains("failed lookup"), "{refusal}");
    }

    /// An unreadable marker refuses the call it hit, as its own diagnostic,
    /// and holds nothing over: once it reads again, each run is answered as
    /// it was.
    #[tokio::test]
    async fn an_unreadable_marker_refuses_only_the_calls_it_hit() {
        let dbs = dbs().await;
        with_generation(&dbs, generation(RUNNING)).await;
        dbs.local
            .execute(
                "ALTER TABLE inference_generation_runs RENAME TO held_generation_runs",
                (),
            )
            .await
            .unwrap();
        for run in ["r-builder", "r-agent"] {
            let refusal = refuse_in(&dbs, &request(run, "read", json!({"path": "x"})))
                .await
                .unwrap();
            assert!(
                refusal.contains("could not read whether run"),
                "{run}: {refusal}"
            );
            assert!(refusal.contains("not a scope violation"), "{refusal}");
        }
        dbs.local
            .execute(
                "ALTER TABLE held_generation_runs RENAME TO inference_generation_runs",
                (),
            )
            .await
            .unwrap();
        assert!(refuse_in(&dbs, &request("r-builder", "write", file_edit()))
            .await
            .is_none());
        assert!(refuse_in(&dbs, &request("r-agent", "write", draft()))
            .await
            .is_none());
        let scoped = refuse_in(&dbs, &request("r-agent", "write", file_edit()))
            .await
            .unwrap();
        assert!(scoped.contains("This session generates"), "{scoped}");
    }

    /// A relaunch is a new run, held to the scope from the marker its launch
    /// writes before the run can call; the bound job's relaunch is still its
    /// bound session.
    #[tokio::test]
    async fn a_relaunched_run_is_scoped_from_its_marker() {
        let dbs = dbs().await;
        with_generation(&dbs, generation(RUNNING)).await;
        let later = chrono::Utc::now().timestamp();
        for (run, job) in [("r-relaunch", "j-agent"), ("r-task", "j-second")] {
            dbs.local
                .execute(
                    "INSERT INTO runs (id, project_id, job_id, issue_id, status, created_at, updated_at)
                     VALUES (?1, 'p', ?2, 'i9', 'live', ?3, ?3)",
                    (run, job, later),
                )
                .await
                .unwrap();
            assert_eq!(
                mark_run(&dbs.local, run, Some(job), &["i9"])
                    .await
                    .unwrap()
                    .as_deref(),
                Some("g-1"),
                "{run}"
            );
            assert!(
                refuse_in(&dbs, &request(run, "write", file_edit()))
                    .await
                    .is_some(),
                "{run}"
            );
        }
        assert!(refuse_in(&dbs, &request("r-relaunch", "write", draft()))
            .await
            .is_none());
        assert!(refuse_in(&dbs, &request("r-task", "write", draft()))
            .await
            .is_some());
        // A run on no generation's issue is not marked.
        assert_eq!(
            mark_run(&dbs.local, "r-builder", Some("j-builder"), &["i8"])
                .await
                .unwrap(),
            None
        );
        assert_eq!(marker(&dbs, "r-builder").await, None);
    }

    /// A run's own job's generation is chosen over a newer one on its issue,
    /// and a marker, once written, is never rewritten.
    #[tokio::test]
    async fn a_run_is_marked_for_its_own_jobs_generation_once() {
        let dbs = dbs().await;
        let mut older = generation("gaveUp");
        older.started_at = 1;
        super::super::insert_generation(&dbs.local, &older)
            .await
            .unwrap();
        let mut newer = generation(RUNNING);
        newer.id = "g-2".into();
        newer.job_id = "j-second".into();
        newer.execution_id = Some("e2".into());
        newer.started_at = 5;
        super::super::insert_generation(&dbs.local, &newer)
            .await
            .unwrap();
        let marked = |run: &'static str, job: &'static str| {
            let dbs = &dbs;
            async move { mark_run(&dbs.local, run, Some(job), &["i9"]).await.unwrap() }
        };
        assert_eq!(marked("r-agent", "j-agent").await.as_deref(), Some("g-1"));
        assert_eq!(marked("r-second", "j-second").await.as_deref(), Some("g-2"));
        assert_eq!(marked("r-resumed", "j-other").await.as_deref(), Some("g-2"));
        // Marking again, by either path, keeps the first marker.
        assert_eq!(marked("r-agent", "j-second").await.as_deref(), Some("g-1"));
        assert_eq!(mark_issue_runs(&dbs.local, "g-2", "i9").await.unwrap(), 0);
        assert_eq!(
            marker(&dbs, "r-agent")
                .await
                .map(|m| m.generation_id)
                .as_deref(),
            Some("g-1")
        );
    }

    /// A real orchestrator over [`dbs`] with a running generation on issue 9,
    /// its runs stamped after boot so the ownership fence serves them.
    async fn scoped_orchestrator() -> crate::orchestrator::Orchestrator {
        use crate::services::testing::TestServicesBuilder;

        let dbs = dbs().await;
        with_generation(&dbs, generation(RUNNING)).await;
        let later = chrono::Utc::now().timestamp() + 3600;
        dbs.local
            .execute("UPDATE runs SET created_at = ?1, started_at = ?1", (later,))
            .await
            .unwrap();
        let dir = tempfile::tempdir().unwrap().keep();
        crate::orchestrator::Orchestrator::builder(
            Arc::new(dbs),
            Arc::new(TestServicesBuilder::new().build()),
            dir,
        )
        .build()
    }

    /// The real dispatcher asks the scope after its fences and before any
    /// handler, and answers with the scope's own refusal.
    #[tokio::test]
    async fn the_dispatcher_refuses_a_scoped_call_before_any_handler() {
        let orch = scoped_orchestrator().await;
        let cursors = std::sync::Mutex::new(std::collections::HashMap::new());

        for (run, payload) in [
            ("r-agent", file_edit()),
            ("r-resumed", file_edit()),
            ("r-second", draft()),
            (
                "r-agent",
                json!({"commands": [{"command": "touch /tmp/scope-escape"}]}),
            ),
        ] {
            let tool = if payload.get("commands").is_some() {
                "run"
            } else {
                "write"
            };
            let call = request(run, tool, payload);
            let expected = refuse_in(&orch.db, &call).await.expect("scoped");
            let output = crate::dispatch::dispatch_tool(&orch, &call, &cursors)
                .await
                .into_inner();
            assert_eq!(output.content, expected, "{run} {tool}");
        }
        // An unrelated issue's run is not answered by the scope.
        let builder = request("r-builder", "write", file_edit());
        assert!(refuse_in(&orch.db, &builder).await.is_none());
    }

    /// A pooled Codex call names no run of its own: its server's `cairn-cmd`
    /// carries the pool sentinel, and the call is attributed by its thread.
    /// The scope judges the run that thread is bound to, whatever run the
    /// request claimed, both as the pool's credential binds it and as the
    /// dispatcher resolves it.
    #[tokio::test]
    async fn a_pooled_call_is_scoped_by_the_run_its_thread_is_bound_to() {
        use crate::mcp::run_bearer::{bind_request_to_pool_thread, POOLED_CODEX_RUN_SENTINEL};

        let orch = scoped_orchestrator().await;
        let cursors = std::sync::Mutex::new(std::collections::HashMap::new());
        let (server, _stream, _requests) =
            crate::backends::codex::pool::testing::recording_server(&orch.codex_pool);
        let _agent = server.register_call("thread-agent", "r-agent");
        let _second = server.register_call("thread-second", "r-second");
        let _builder = server.register_call("thread-builder", "r-builder");
        let pooled = |thread: &str, run: &str, tool: &str, payload: Value| {
            let mut call = request(run, tool, payload);
            call.thread_id = Some(thread.into());
            call
        };
        let bound = |thread: &str, tool: &str, payload: Value| {
            let mut call = pooled(thread, POOLED_CODEX_RUN_SENTINEL, tool, payload);
            bind_request_to_pool_thread(
                &orch.codex_pool,
                &orch.run_bearers,
                &mut call,
                server.id(),
            )
            .unwrap();
            call
        };

        // The pool's credential attributes each thread to its own run, and
        // the scope answers for that run: the bound session may draft, its
        // sibling on the issue may not, and neither may edit files.
        let agent_draft = bound("thread-agent", "write", draft());
        assert_eq!(agent_draft.run_id.as_deref(), Some("r-agent"));
        assert!(refuse_in(&orch.db, &agent_draft).await.is_none());
        for (thread, run, payload) in [
            ("thread-agent", "r-agent", file_edit()),
            ("thread-second", "r-second", draft()),
        ] {
            let call = bound(thread, "write", payload.clone());
            assert_eq!(call.run_id.as_deref(), Some(run));
            let expected = refuse_in(&orch.db, &request(run, "write", payload))
                .await
                .expect("scoped");
            let output = crate::dispatch::dispatch_tool(&orch, &call, &cursors)
                .await
                .into_inner();
            assert_eq!(output.content, expected, "{thread}");
        }
        // An unrelated issue's thread is not the scope's to answer.
        let builder = bound("thread-builder", "write", file_edit());
        assert_eq!(builder.run_id.as_deref(), Some("r-builder"));
        assert!(refuse_in(&orch.db, &builder).await.is_none());

        // A generation's thread cannot claim another run past the pool's
        // credential.
        let mut claimed = pooled("thread-agent", "r-builder", "write", file_edit());
        let refusal = bind_request_to_pool_thread(
            &orch.codex_pool,
            &orch.run_bearers,
            &mut claimed,
            server.id(),
        )
        .unwrap_err();
        assert!(refusal.contains("belongs to run r-agent"), "{refusal}");

        // Nor past the dispatcher, which attributes a resolvable thread to its
        // own run before any fence: a stale unscoped `run_id` beside the
        // generation's thread is refused as the generation's run.
        let stale = pooled("thread-agent", "r-builder", "write", file_edit());
        let expected = refuse_in(&orch.db, &request("r-agent", "write", file_edit()))
            .await
            .expect("scoped");
        let output = crate::dispatch::dispatch_tool(&orch, &stale, &cursors)
            .await
            .into_inner();
        assert_eq!(output.content, expected);
    }

    /// A process on another machine reaches the runner through its executor's
    /// relay, and the relay context the runner opened for it says which run it
    /// speaks for. A call relayed for a generation's run is held to that run's
    /// scope exactly as a local one is.
    #[tokio::test]
    async fn a_relayed_remote_call_is_scoped_by_the_run_its_relay_speaks_for() {
        use crate::fleet::resident_relay::{
            authorize_resident_relay, RegisteredAuthority, ResidentRelayCall,
        };
        use cairn_common::executor_protocol::{
            RelayAuthorization, ResidencyHolder, ResidentRelayId,
        };

        let orch = scoped_orchestrator().await;
        let cursors = std::sync::Mutex::new(std::collections::HashMap::new());
        // A relay opened on machine-b's link 7 for one process of `run`'s job,
        // whose start was answered at generation 4.
        let open = |run: &str, job: &str| {
            let holder = ResidencyHolder::Job { job_id: job.into() };
            let mut relays = orch.fleet.resident_relays();
            let minted = relays.register(
                "machine-b",
                &holder,
                "shell",
                RegisteredAuthority::Run {
                    run_id: Some(run.into()),
                },
            );
            let relay_id = minted.grant.relay_id;
            relays.bind(&relay_id, "machine-b", 7).unwrap();
            relays.learn_generation(&relay_id, 4);
            (
                relay_id,
                holder,
                RelayAuthorization(format!("Bearer {}", minted.capability.unwrap())),
            )
        };
        let agent = open("r-agent", "j-agent");
        let second = open("r-second", "j-second");
        let builder = open("r-builder", "j-builder");
        let relayed = |relay: &(ResidentRelayId, ResidencyHolder, RelayAuthorization),
                       claimed: Option<&str>,
                       payload: Value| {
            let mut call = request("", "write", payload);
            call.run_id = claimed.map(str::to_string);
            authorize_resident_relay(
                &orch,
                "machine-b",
                7,
                &ResidentRelayCall {
                    relay_id: &relay.0,
                    holder: &relay.1,
                    process_key: "shell",
                    process_generation: 4,
                    authorization: &relay.2,
                },
                &mut call,
            )
            .map(|()| call)
        };

        let agent_draft = relayed(&agent, None, draft()).unwrap();
        assert_eq!(agent_draft.run_id.as_deref(), Some("r-agent"));
        assert!(refuse_in(&orch.db, &agent_draft).await.is_none());
        for (relay, run, payload) in [
            (&agent, "r-agent", file_edit()),
            (&second, "r-second", draft()),
        ] {
            let call = relayed(relay, None, payload.clone()).unwrap();
            assert_eq!(call.run_id.as_deref(), Some(run));
            let expected = refuse_in(&orch.db, &request(run, "write", payload))
                .await
                .expect("scoped");
            let output = crate::dispatch::dispatch_tool(&orch, &call, &cursors)
                .await
                .into_inner();
            assert_eq!(output.content, expected, "{run}");
        }
        let unrelated = relayed(&builder, None, file_edit()).unwrap();
        assert_eq!(unrelated.run_id.as_deref(), Some("r-builder"));
        assert!(refuse_in(&orch.db, &unrelated).await.is_none());

        // The generation's relay credential cannot speak for another run.
        let refusal = relayed(&agent, Some("r-builder"), file_edit()).unwrap_err();
        assert!(refusal.contains("speaks for run r-agent"), "{refusal}");
    }
}
