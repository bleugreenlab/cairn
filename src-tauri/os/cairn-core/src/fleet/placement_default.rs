//! A node's placement default: one declared executor selector that a home's
//! own free-placement `run` batches, terminals and REPLs inherit when a call
//! names no `executor` of its own (cairn/5920).
//!
//! This module is the single owner of the `placement_defaults` table. Every
//! surface that sets, reads, applies or clears a default goes through it: the
//! `executor` patch on a node, task or thread, the overviews and the
//! orientation block, the run handler, terminal and REPL creates, and fleet
//! removal.
//!
//! A default is a declaration, never inferred from where earlier batches
//! landed, and a pin rather than a preference: when the machine cannot serve,
//! the work is refused naming the default and how to override or clear it. It
//! is deliberately invisible to two things. Sub-agent tasks do not inherit it,
//! because a task is its own job and finds its own row or none. Check lanes
//! never see it, because the run handler keeps it out of the batch's
//! `CAIRN_BATCH_EXECUTOR` (tenet 1233).

use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_common::executor_protocol::{ExecutorSelector, EXECUTOR_PLATFORMS};
use cairn_db::turso::params;

/// What the orientation block adds to the default's line, so an agent knows
/// what inherits it, what does not, and what it is for.
const ORIENTATION_GUIDANCE: &str = "Free-placement run batches, terminals and REPLs you create go there unless a call names its own executor; sub-agent tasks do not inherit it (name the machine in the task prompt). It is for machine-specific claims and live-process continuity, not cache warmth. Clear with `write cairn:~ patch {executor:null}`.";

/// The label a result carries beside the machine when the default placed it.
pub(crate) const NODE_DEFAULT_LABEL: &str = "(node default)";

/// Whose default a row is.
///
/// A thread's session is keyed by the thread, because a thread can change
/// session jobs and its default is the thread's. Every other home (an
/// execution node, a sub-agent task, a thread's task) is keyed by its own job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DefaultOwner {
    Job(String),
    Thread(String),
}

impl DefaultOwner {
    /// The owner of `job_id`'s default: its thread when the job is a thread's
    /// session, the job itself otherwise.
    pub(crate) async fn for_job_conn(
        conn: &cairn_db::storage::TrackedConnection,
        job_id: &str,
    ) -> DbResult<Self> {
        let mut rows = conn
            .query(
                &format!(
                    "SELECT j.thread_id FROM jobs j
                     WHERE j.id = ?1 AND j.thread_id IS NOT NULL AND {}",
                    crate::threads::SESSION_JOB_SHAPE
                ),
                params![job_id],
            )
            .await?;
        Ok(match rows.next().await? {
            Some(row) => Self::Thread(row.text(0)?),
            None => Self::Job(job_id.to_string()),
        })
    }

    pub(crate) async fn for_job(db: &LocalDb, job_id: &str) -> Result<Self, String> {
        let job_id = job_id.to_string();
        db.read(move |conn| Box::pin(async move { Self::for_job_conn(conn, &job_id).await }))
            .await
            .map_err(|error| error.to_string())
    }

    fn column(&self) -> (&'static str, &str) {
        match self {
            Self::Job(id) => ("job_id", id),
            Self::Thread(id) => ("thread_id", id),
        }
    }
}

/// Why and when the fleet cleared a default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Cleared {
    pub(crate) reason: String,
    pub(crate) at: i64,
}

/// One home's placement default as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacementDefault {
    pub(crate) selector: ExecutorSelector,
    /// The machine a `{name}` selector resolved to when it was set.
    pub(crate) executor_id: Option<String>,
    /// The writer's canonical home URI, or `operator`.
    pub(crate) set_by: String,
    /// Unix seconds.
    pub(crate) set_at: i64,
    /// Set once the fleet cleared it; a cleared default places nothing.
    pub(crate) cleared: Option<Cleared>,
}

impl PlacementDefault {
    pub(crate) fn is_active(&self) -> bool {
        self.cleared.is_none()
    }

    /// What the default names: the machine, or the selector in a refusal's words.
    pub(crate) fn target(&self) -> String {
        self.selector
            .name
            .clone()
            .unwrap_or_else(|| self.selector.describe())
    }

    fn set_phrase(&self) -> String {
        format!("set {} by {}", stamp(self.set_at), self.set_by)
    }

    /// The one-line render shared by every overview.
    pub(crate) fn line(&self) -> String {
        match &self.cleared {
            None => format!(
                "Placement default: {} ({}).",
                self.target(),
                self.set_phrase()
            ),
            Some(cleared) => format!(
                "Placement default: none ({} cleared {}: {}).",
                self.target(),
                stamp(cleared.at),
                cleared.reason
            ),
        }
    }

    /// The orientation block's line: the overview line and what it means. A
    /// cleared default has nothing to orient by and is omitted.
    pub(crate) fn orientation_line(&self) -> Option<String> {
        self.is_active()
            .then(|| format!("{} {ORIENTATION_GUIDANCE}", self.line()))
    }

    /// Appended to every refusal of work the default placed, so the refusal
    /// says where the pin came from and the two ways out of it.
    pub(crate) fn refusal_suffix(&self) -> String {
        format!(
            "{} is this node's placement default ({}). Override it for one call with `executor:{{...}}` on the run, or clear it with `write cairn:~ patch {{executor:null}}`.",
            self.target(),
            self.set_phrase()
        )
    }

    /// Appended to a terminal or REPL create the default placed and the
    /// environment refused. The create already named no `executor`, so the way
    /// out is to name one on it, or to clear the default.
    pub(crate) fn resident_refusal_suffix(&self) -> String {
        format!(
            "{} is this node's placement default ({}), which this create inherited. Name `executor:{{...}}` on the create to place it elsewhere, or clear the default with `write cairn:~ patch {{executor:null}}`.",
            self.target(),
            self.set_phrase()
        )
    }

    fn clear_notice(&self) -> Option<String> {
        let cleared = self.cleared.as_ref()?;
        Some(format!(
            "Note: this node's placement default ({}) was cleared {}: {}. This node's batches now place freely unless a call names a machine; set a new default with `write cairn:~ patch {{executor:{{name:\"...\"}}}}` if the work still needs one.",
            self.target(),
            stamp(cleared.at),
            cleared.reason
        ))
    }
}

fn stamp(unix_seconds: i64) -> String {
    crate::clock::stamp(unix_seconds).unwrap_or_else(|| format!("unix {unix_seconds}"))
}

fn now_seconds() -> i64 {
    i64::try_from(crate::fleet::unix_time_ms() / 1_000).unwrap_or(i64::MAX)
}

/// What setting a default produced: the stored default, and anything the
/// writer should know about the machine it names right now.
#[derive(Debug, Clone)]
pub(crate) struct SetOutcome {
    pub(crate) default: PlacementDefault,
    pub(crate) warning: Option<String>,
}

impl SetOutcome {
    pub(crate) fn summary(&self) -> String {
        match &self.warning {
            Some(warning) => format!("{} {warning}", self.default.line()),
            None => self.default.line(),
        }
    }
}

/// Parse a patch's `executor` object with the rules a per-call selector
/// follows: `name` or `os` (never both), optionally `requiredToolchains`,
/// never empty, and an `os` Cairn places work on.
pub(crate) fn parse_selector(value: &serde_json::Value) -> Result<ExecutorSelector, String> {
    if value.as_object().is_some_and(|object| object.is_empty()) {
        return Err(
            "payload.executor must name a machine or platform; use `executor:null` to clear the default"
                .to_string(),
        );
    }
    let selector = crate::fleet::residency::parse_resident_selector(value)?;
    if let Some(os) = &selector.os {
        if !EXECUTOR_PLATFORMS.contains(&os.as_str()) {
            return Err(format!(
                "payload.executor: os {os:?} is not a platform Cairn places work on; use one of {}",
                EXECUTOR_PLATFORMS.join(", ")
            ));
        }
    }
    Ok(selector)
}

/// A patch's `executor` object, validated and resolved, not yet stored.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedDefault {
    selector: ExecutorSelector,
    executor_id: Option<String>,
    warning: Option<String>,
}

/// Validate and resolve a patch's `executor` object without writing anything.
///
/// A `{name}` is resolved to the machine it addresses now and stored under its
/// public name and identity; a name that addresses nothing is refused with the
/// ones that do. A machine that exists but cannot take work right now is
/// accepted, and the outcome says so: the default is a declaration about where
/// the work belongs, and its refusals will say the same thing every time.
pub(crate) fn resolve(
    orch: &Orchestrator,
    value: &serde_json::Value,
) -> Result<ResolvedDefault, String> {
    let mut selector = parse_selector(value)?;
    let mut executor_id = None;
    let mut warning = None;
    if let Some(name) = selector.name.clone() {
        let id = crate::fleet::management::resolve_executor_reference(orch, &name)?;
        let public = crate::fleet::management::public_name(orch, &id);
        if orch.fleet.is_drained(&id) {
            warning = Some(format!(
                "{public} is drained now; batches will be refused until an operator clears the drain or this default changes."
            ));
        } else if orch.fleet.executor_public_name(&id).is_none() {
            warning = Some(format!(
                "{public} is not connected now; batches will be refused until it reconnects or this default changes."
            ));
        }
        selector.name = Some(public);
        executor_id = Some(id);
    }
    Ok(ResolvedDefault {
        selector,
        executor_id,
        warning,
    })
}

/// Store a resolved default as `owner`'s.
pub(crate) async fn set(
    db: &LocalDb,
    owner: &DefaultOwner,
    resolved: ResolvedDefault,
    set_by: &str,
) -> Result<SetOutcome, String> {
    let default = store(
        db,
        owner,
        &resolved.selector,
        resolved.executor_id.as_deref(),
        set_by,
        now_seconds(),
    )
    .await?;
    Ok(SetOutcome {
        default,
        warning: resolved.warning,
    })
}

/// Write `owner`'s default, replacing any it had (cleared or not).
pub(crate) async fn store(
    db: &LocalDb,
    owner: &DefaultOwner,
    selector: &ExecutorSelector,
    executor_id: Option<&str>,
    set_by: &str,
    now: i64,
) -> Result<PlacementDefault, String> {
    let selector_json = serde_json::to_string(selector).map_err(|error| error.to_string())?;
    let (column, owner_id) = owner.column();
    let (owner_id, executor, set_by_owned) = (
        owner_id.to_string(),
        executor_id.map(str::to_string),
        set_by.to_string(),
    );
    db.write(move |conn| {
        let (owner_id, selector_json, executor, set_by) = (
            owner_id.clone(),
            selector_json.clone(),
            executor.clone(),
            set_by_owned.clone(),
        );
        Box::pin(async move {
            conn.execute(
                &format!("DELETE FROM placement_defaults WHERE {column} = ?1"),
                params![owner_id.as_str()],
            )
            .await?;
            conn.execute(
                &format!(
                    "INSERT INTO placement_defaults
                        (id, {column}, selector, executor_id, set_by, set_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
                ),
                params![
                    uuid::Uuid::new_v4().to_string(),
                    owner_id.as_str(),
                    selector_json.as_str(),
                    executor.as_deref(),
                    set_by.as_str(),
                    now
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("could not store the placement default: {error}"))?;
    Ok(PlacementDefault {
        selector: selector.clone(),
        executor_id: executor_id.map(str::to_string),
        set_by: set_by.to_string(),
        set_at: now,
        cleared: None,
    })
}

/// Remove `owner`'s default, answering whether it had one.
pub(crate) async fn clear(db: &LocalDb, owner: &DefaultOwner) -> Result<bool, String> {
    let (column, owner_id) = owner.column();
    db.execute(
        format!("DELETE FROM placement_defaults WHERE {column} = ?1"),
        params![owner_id],
    )
    .await
    .map(|removed| removed > 0)
    .map_err(|error| format!("could not clear the placement default: {error}"))
}

fn default_from_row(row: &cairn_db::turso::Row) -> DbResult<PlacementDefault> {
    let selector_json = row.text(0)?;
    // A selector this build cannot read is shown and applied as nothing it
    // could misplace work by: the empty selector names no machine, and the
    // overview still prints the row's text through `describe`.
    let selector = serde_json::from_str(&selector_json).unwrap_or_default();
    let cleared = match (row.opt_text(5)?, row.opt_i64(6)?) {
        (Some(reason), Some(at)) => Some(Cleared { reason, at }),
        _ => None,
    };
    Ok(PlacementDefault {
        selector,
        executor_id: row.opt_text(1)?,
        set_by: row.text(2)?,
        set_at: row.i64(3)?,
        cleared,
    })
}

const DEFAULT_COLUMNS: &str =
    "selector, executor_id, set_by, set_at, clear_noticed, cleared_reason, cleared_at";

/// `owner`'s default as stored, cleared or not.
pub(crate) async fn load_conn(
    conn: &cairn_db::storage::TrackedConnection,
    owner: &DefaultOwner,
) -> DbResult<Option<PlacementDefault>> {
    let (column, owner_id) = owner.column();
    let mut rows = conn
        .query(
            &format!("SELECT {DEFAULT_COLUMNS} FROM placement_defaults WHERE {column} = ?1"),
            params![owner_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(default_from_row(&row)?)),
        None => Ok(None),
    }
}

pub(crate) async fn load(
    db: &LocalDb,
    owner: &DefaultOwner,
) -> Result<Option<PlacementDefault>, String> {
    let owner = owner.clone();
    db.read(move |conn| Box::pin(async move { load_conn(conn, &owner).await }))
        .await
        .map_err(|error| error.to_string())
}

/// The default `job_id`'s own work places by, and the note owed to its next
/// result when the fleet cleared the one it had. The note is marked delivered
/// here, before placement, so a batch that never settles a result (its call
/// abandoned, its placement task lost) consumes it unseen; the overview's
/// cleared line still says what happened.
///
/// A lookup failure is logged and read as "no default": a default is a
/// convenience over naming the machine per call, and a store hiccup must not
/// refuse work that never asked for one.
pub(crate) async fn for_job_work(
    db: &LocalDb,
    job_id: &str,
) -> (Option<PlacementDefault>, Option<String>) {
    let owner = match DefaultOwner::for_job(db, job_id).await {
        Ok(owner) => owner,
        Err(error) => {
            log::warn!("could not resolve the placement-default owner of job {job_id}: {error}");
            return (None, None);
        }
    };
    let default = match load(db, &owner).await {
        Ok(default) => default,
        Err(error) => {
            log::warn!("could not read the placement default of job {job_id}: {error}");
            return (None, None);
        }
    };
    let notice = match default.as_ref() {
        Some(default) if !default.is_active() => {
            take_clear_notice(db, &owner).await.unwrap_or_else(|error| {
                log::warn!(
                    "could not deliver the cleared placement default of job {job_id}: {error}"
                );
                None
            })
        }
        _ => None,
    };
    (default.filter(PlacementDefault::is_active), notice)
}

/// The active default of the job a node- or task-scoped resource belongs to,
/// for a terminal or REPL that job creates without naming a machine. A job
/// that cannot be resolved has none: the create then reports its own error.
pub(crate) async fn for_job_resource(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: Option<&str>,
) -> Option<PlacementDefault> {
    let db = orch.db.for_project(project).await;
    let job_id = crate::resources::resolve_node_or_task_job_id_for_read(
        &db, project, number, exec_seq, node_id, task_name,
    )
    .await
    .ok()?;
    for_job(&db, &job_id).await
}

/// The active default `job_id`'s resident processes are placed by. Unlike
/// [`for_job_work`] this never takes the cleared-default note: that note is
/// owed to the node's next run result.
pub(crate) async fn for_job(db: &LocalDb, job_id: &str) -> Option<PlacementDefault> {
    let owner = DefaultOwner::for_job(db, job_id).await.ok()?;
    load(db, &owner)
        .await
        .unwrap_or_else(|error| {
            log::warn!("could not read the placement default of job {job_id}: {error}");
            None
        })
        .filter(PlacementDefault::is_active)
}

/// A resident create's selector: its own, else the job's default. The second
/// half is the default that supplied it, for the label and refusals.
pub(crate) fn resident_selector(
    own: Option<ExecutorSelector>,
    default: impl FnOnce() -> Option<PlacementDefault>,
) -> (Option<ExecutorSelector>, Option<PlacementDefault>) {
    match own.filter(|selector| !selector.is_empty()) {
        Some(selector) => (Some(selector), None),
        None => match default() {
            Some(default) => (Some(default.selector.clone()), Some(default)),
            None => (None, None),
        },
    }
}

/// A resident create's result under the default that placed it: labelled when
/// it started, explained when it was refused.
pub(crate) fn resident_outcome(
    default: Option<&PlacementDefault>,
    outcome: Result<String, String>,
) -> Result<String, String> {
    let Some(default) = default else {
        return outcome;
    };
    outcome.map(|text| label_landing(&text)).map_err(|error| {
        format!(
            "{}\n\n{}",
            name_the_machine_instead(&error),
            default.resident_refusal_suffix()
        )
    })
}

/// A resident refusal's "create it without `executor`" advice, rewritten for a
/// create that already named none: under a default that create inherits the
/// same selector and is refused again, so the way to join the environment is
/// to name its machine.
fn name_the_machine_instead(refusal: &str) -> String {
    const JOIN: &str = " without `executor` to join it on **";
    if let Some(at) = refusal.find(JOIN) {
        let machine_start = at + JOIN.len();
        if let Some(machine_len) = refusal[machine_start..].find("**") {
            let machine = &refusal[machine_start..machine_start + machine_len];
            if let Some(create) = refusal[..at].rfind("or create ") {
                let surface = &refusal[create + "or create ".len()..at];
                return format!(
                    "{}or create {surface} with `executor:{{name:\"{machine}\"}}` to join it on **{machine}**{}",
                    &refusal[..create],
                    &refusal[machine_start + machine_len + "**".len()..]
                );
            }
        }
    }
    refusal.replace(
        "or create it without `executor`",
        "or name the machine it runs on in this create's `executor`",
    )
}

/// The note that `owner`'s default was cleared by the fleet, the first time it
/// is asked for; `None` every time after, and for a default never cleared.
pub(crate) async fn take_clear_notice(
    db: &LocalDb,
    owner: &DefaultOwner,
) -> Result<Option<String>, String> {
    let owner = owner.clone();
    db.write(move |conn| {
        let owner = owner.clone();
        Box::pin(async move {
            let (column, owner_id) = owner.column();
            let Some(default) = load_conn(conn, &owner).await? else {
                return Ok(None);
            };
            let mut rows = conn
                .query(
                    &format!(
                        "SELECT 1 FROM placement_defaults
                         WHERE {column} = ?1 AND cleared_at IS NOT NULL AND clear_noticed = 0"
                    ),
                    params![owner_id],
                )
                .await?;
            if rows.next().await?.is_none() {
                return Ok(None);
            }
            drop(rows);
            conn.execute(
                &format!("UPDATE placement_defaults SET clear_noticed = 1 WHERE {column} = ?1"),
                params![owner_id],
            )
            .await?;
            Ok(default.clear_notice())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Clear every default that named `executor_id` by name, because the machine
/// left the fleet. A default naming a platform names no machine and stays.
/// Answers how many it cleared.
pub(crate) async fn clear_for_removed_executor(
    db: &LocalDb,
    executor_id: &str,
    name: &str,
    now: i64,
) -> Result<u64, String> {
    db.execute(
        "UPDATE placement_defaults
            SET cleared_reason = ?2, cleared_at = ?3, clear_noticed = 0
          WHERE executor_id = ?1 AND cleared_at IS NULL",
        params![
            executor_id,
            format!("executor {name} was removed from the fleet"),
            now
        ],
    )
    .await
    .map_err(|error| error.to_string())
}

/// Clear the defaults naming a removed machine in every database this runner
/// holds. A failure in one is logged and the rest still clear.
pub(crate) async fn clear_everywhere_for_removed_executor(
    orch: &Orchestrator,
    executor_id: &str,
    name: &str,
) {
    let now = now_seconds();
    for db in orch.db.all_dbs().await {
        if let Err(error) = clear_for_removed_executor(&db, executor_id, name, now).await {
            log::warn!(
                "could not clear placement defaults naming removed executor {name}: {error}"
            );
        }
    }
}

/// Mark a create result's machine as the node default's choice: ` on **bbs**`
/// becomes ` on **bbs** (node default)`. A result naming no machine is left
/// as it is.
pub(crate) fn label_landing(text: &str) -> String {
    let Some(start) = text.find(" on **") else {
        return text.to_string();
    };
    let name_start = start + " on **".len();
    let Some(name_len) = text[name_start..].find("**") else {
        return text.to_string();
    };
    let end = name_start + name_len + "**".len();
    format!("{} {NODE_DEFAULT_LABEL}{}", &text[..end], &text[end..])
}

/// Who is writing a default: the calling agent's canonical home, or
/// `operator` for the app or CLI.
pub(crate) async fn writer(
    orch: &Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
) -> String {
    let from_cli = request
        .payload
        .get("_cairn_origin")
        .and_then(|value| value.as_str())
        == Some("cli");
    if from_cli || request.run_id.is_none() {
        return "operator".to_string();
    }
    crate::mcp::handlers::run_context::lookup_home_uri_routed(&orch.db, request)
        .await
        .unwrap_or_else(|_| "operator".to_string())
}

/// Apply a patch's `executor` value to `owner`: an object sets the default,
/// `null` clears it. Answers the line the write result echoes.
pub(crate) async fn apply_patch(
    orch: &Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
    db: &LocalDb,
    owner: &DefaultOwner,
    value: &serde_json::Value,
    dry_run: bool,
) -> Result<String, String> {
    let prepared = prepare_patch(orch, value)?;
    commit_patch(orch, request, db, owner, prepared, dry_run).await
}

/// A patch's `executor` value, validated: set this, or clear.
#[derive(Debug, Clone)]
pub(crate) enum PreparedPatch {
    Set(ResolvedDefault),
    Clear,
}

/// Validate a patch's `executor` value without writing anything, so a patch
/// that carries it beside other keys can refuse before any of them applies.
pub(crate) fn prepare_patch(
    orch: &Orchestrator,
    value: &serde_json::Value,
) -> Result<PreparedPatch, String> {
    if value.is_null() {
        return Ok(PreparedPatch::Clear);
    }
    if !value.is_object() {
        return Err(
            "payload.executor must be an object ({name} or {os}, optionally requiredToolchains) or null to clear"
                .to_string(),
        );
    }
    resolve(orch, value).map(PreparedPatch::Set)
}

/// Write a prepared patch to `owner`. Answers the line the write result echoes.
pub(crate) async fn commit_patch(
    orch: &Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
    db: &LocalDb,
    owner: &DefaultOwner,
    prepared: PreparedPatch,
    dry_run: bool,
) -> Result<String, String> {
    match prepared {
        PreparedPatch::Clear if dry_run => Ok("Would clear the placement default".to_string()),
        PreparedPatch::Clear => Ok(match clear(db, owner).await? {
            true => "Placement default: none (cleared).".to_string(),
            false => "Placement default: none (there was none to clear).".to_string(),
        }),
        PreparedPatch::Set(resolved) if dry_run => Ok(format!(
            "Would set the placement default to {}",
            resolved.selector.describe()
        )),
        PreparedPatch::Set(resolved) => {
            let set_by = writer(orch, request).await;
            Ok(set(db, owner, resolved, &set_by).await?.summary())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn seeded_db() -> LocalDb {
        let db = crate::storage::migrated_test_db("placement-defaults.db").await;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p', 'default', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);
             INSERT INTO threads (id, project_id, name, created_at, updated_at)
               VALUES ('t-exec', 'p', 'execution', 2, 2);
             INSERT INTO jobs (id, parent_job_id, project_id, status, node_name, uri_segment, created_at, updated_at)
               VALUES ('j-node', NULL, 'p', 'running', 'builder', 'builder', 3, 3);
             INSERT INTO jobs (id, parent_job_id, thread_id, project_id, status, node_name, uri_segment, created_at, updated_at)
               VALUES ('j-session', NULL, 't-exec', 'p', 'running', 'thread', 'thread', 4, 4);
             INSERT INTO jobs (id, parent_job_id, thread_id, project_id, status, node_name, uri_segment, created_at, updated_at)
               VALUES ('j-task', 'j-session', 't-exec', 'p', 'running', 'probe', 'probe', 5, 5);",
        )
        .await
        .unwrap();
        db
    }

    fn named(name: &str) -> ExecutorSelector {
        ExecutorSelector {
            name: Some(name.to_string()),
            ..ExecutorSelector::default()
        }
    }

    #[tokio::test]
    async fn a_thread_session_owns_by_thread_and_every_other_job_by_itself() {
        let db = seeded_db().await;
        assert_eq!(
            DefaultOwner::for_job(&db, "j-session").await.unwrap(),
            DefaultOwner::Thread("t-exec".into())
        );
        assert_eq!(
            DefaultOwner::for_job(&db, "j-task").await.unwrap(),
            DefaultOwner::Job("j-task".into())
        );
        assert_eq!(
            DefaultOwner::for_job(&db, "j-node").await.unwrap(),
            DefaultOwner::Job("j-node".into())
        );
    }

    #[tokio::test]
    async fn a_default_round_trips_for_each_owner_kind_and_clears() {
        let db = seeded_db().await;
        for owner in [
            DefaultOwner::Job("j-node".into()),
            DefaultOwner::Thread("t-exec".into()),
            DefaultOwner::Job("j-task".into()),
        ] {
            let stored = store(
                &db,
                &owner,
                &named("bbs"),
                Some("exec-bbs"),
                "operator",
                100,
            )
            .await
            .unwrap();
            assert_eq!(load(&db, &owner).await.unwrap(), Some(stored));
            // Setting again replaces rather than duplicating.
            let os = ExecutorSelector {
                os: Some("linux".into()),
                ..ExecutorSelector::default()
            };
            store(&db, &owner, &os, None, "operator", 200)
                .await
                .unwrap();
            let loaded = load(&db, &owner).await.unwrap().unwrap();
            assert_eq!(loaded.selector, os);
            assert_eq!(loaded.set_at, 200);
            assert!(clear(&db, &owner).await.unwrap());
            assert_eq!(load(&db, &owner).await.unwrap(), None);
            assert!(!clear(&db, &owner).await.unwrap());
        }
    }

    #[tokio::test]
    async fn a_task_never_reads_its_parents_default() {
        let db = seeded_db().await;
        store(
            &db,
            &DefaultOwner::Thread("t-exec".into()),
            &named("bbs"),
            Some("exec-bbs"),
            "operator",
            100,
        )
        .await
        .unwrap();
        let (session, _) = for_job_work(&db, "j-session").await;
        assert_eq!(session.map(|default| default.target()), Some("bbs".into()));
        assert_eq!(for_job_work(&db, "j-task").await, (None, None));
    }

    #[tokio::test]
    async fn removal_clears_name_defaults_for_that_machine_once_and_leaves_platforms() {
        let db = seeded_db().await;
        let node = DefaultOwner::Job("j-node".into());
        let thread = DefaultOwner::Thread("t-exec".into());
        let task = DefaultOwner::Job("j-task".into());
        store(&db, &node, &named("bbs"), Some("exec-bbs"), "operator", 100)
            .await
            .unwrap();
        store(
            &db,
            &task,
            &named("fedora"),
            Some("exec-fedora"),
            "operator",
            100,
        )
        .await
        .unwrap();
        let os = ExecutorSelector {
            os: Some("linux".into()),
            ..ExecutorSelector::default()
        };
        store(&db, &thread, &os, None, "operator", 100)
            .await
            .unwrap();

        assert_eq!(
            clear_for_removed_executor(&db, "exec-bbs", "bbs", 300)
                .await
                .unwrap(),
            1
        );
        let cleared = load(&db, &node).await.unwrap().unwrap();
        assert_eq!(
            cleared.cleared,
            Some(Cleared {
                reason: "executor bbs was removed from the fleet".into(),
                at: 300
            })
        );
        assert!(cleared
            .line()
            .starts_with("Placement default: none (bbs cleared "));
        assert!(cleared
            .line()
            .ends_with(": executor bbs was removed from the fleet)."));
        assert!(cleared.orientation_line().is_none());
        assert!(load(&db, &task).await.unwrap().unwrap().is_active());
        assert!(load(&db, &thread).await.unwrap().unwrap().is_active());

        // The node's next work places freely and is told so exactly once.
        let (default, notice) = for_job_work(&db, "j-node").await;
        assert_eq!(default, None);
        let notice = notice.expect("the first result after removal carries the note");
        assert!(notice.contains("(bbs) was cleared"), "{notice}");
        assert!(notice.contains("executor bbs was removed from the fleet"));
        assert_eq!(for_job_work(&db, "j-node").await, (None, None));
        assert_eq!(take_clear_notice(&db, &node).await.unwrap(), None);
        // A second removal does not re-arm a default already cleared.
        assert_eq!(
            clear_for_removed_executor(&db, "exec-bbs", "bbs", 400)
                .await
                .unwrap(),
            0
        );
    }

    #[test]
    fn the_lines_name_the_machine_who_set_it_and_the_way_out() {
        let default = PlacementDefault {
            selector: named("bbs"),
            executor_id: Some("exec-bbs".into()),
            set_by: "cairn://p/cairn/execution".into(),
            set_at: 100,
            cleared: None,
        };
        let line = default.line();
        assert!(line.starts_with("Placement default: bbs (set "), "{line}");
        assert!(line.ends_with(" by cairn://p/cairn/execution)."), "{line}");
        let orientation = default.orientation_line().unwrap();
        assert!(orientation.starts_with(&line));
        assert!(orientation.contains("sub-agent tasks do not inherit it"));
        assert!(orientation.contains("not cache warmth"));
        let suffix = default.refusal_suffix();
        assert!(suffix.starts_with("bbs is this node's placement default (set "));
        assert!(suffix.contains("`write cairn:~ patch {executor:null}`"));
        let platform = PlacementDefault {
            selector: ExecutorSelector {
                os: Some("linux".into()),
                required_toolchains: vec!["rust".into()],
                ..ExecutorSelector::default()
            },
            executor_id: None,
            ..default
        };
        assert!(platform
            .line()
            .starts_with("Placement default: os linux with toolchains rust (set "));
    }

    #[test]
    fn a_patch_selector_follows_the_per_call_rules() {
        assert_eq!(
            parse_selector(&json!({"name": "bbs"})).unwrap(),
            named("bbs")
        );
        assert!(parse_selector(&json!({"os": "linux", "requiredToolchains": ["rust"]})).is_ok());
        let both = parse_selector(&json!({"name": "bbs", "os": "linux"})).unwrap_err();
        assert!(both.contains("never both"), "{both}");
        let empty = parse_selector(&json!({})).unwrap_err();
        assert!(empty.contains("executor:null"), "{empty}");
        let platform = parse_selector(&json!({"os": "plan9"})).unwrap_err();
        assert!(platform.contains("linux, macos, windows"), "{platform}");
        assert!(parse_selector(&json!({"containment": {}})).is_err());
    }

    #[test]
    fn a_create_result_marks_the_machine_the_default_chose() {
        assert_eq!(
            label_landing("Started terminal dev on **bbs**"),
            "Started terminal dev on **bbs** (node default)"
        );
        assert_eq!(
            label_landing(" on **bbs**. This job's environment moved here from **mac**"),
            " on **bbs** (node default). This job's environment moved here from **mac**"
        );
        assert_eq!(
            label_landing("Started terminal dev"),
            "Started terminal dev"
        );
    }

    /// Under a default, "create it without `executor`" is advice that gets the
    /// same refusal again, so the refusal names the machine to ask for and how
    /// to clear the default instead.
    #[test]
    fn a_resident_refusal_under_a_default_names_a_way_out_that_works() {
        let default = PlacementDefault {
            selector: named("bbs"),
            executor_id: Some("exec-bbs".into()),
            set_by: "operator".into(),
            set_at: 100,
            cleared: None,
        };
        let refused = resident_outcome(
            Some(&default),
            Err("terminal `dev` was not started: REPL `x` holds this job's environment on **mac**, which does not match **bbs**. Stop it first, or create terminal `dev` without `executor` to join it on **mac**. Nothing was started.".into()),
        )
        .unwrap_err();
        assert!(
            refused.contains("or create terminal `dev` with `executor:{name:\"mac\"}` to join it on **mac**. Nothing was started."),
            "{refused}"
        );
        assert!(!refused.contains("without `executor`"), "{refused}");
        assert!(refused.contains("which this create inherited"), "{refused}");
        assert!(
            refused.contains("`write cairn:~ patch {executor:null}`"),
            "{refused}"
        );
        assert!(!refused.contains("on the run"), "{refused}");

        let running = resident_outcome(
            Some(&default),
            Err("REPL `x` is already running on **mac**, which does not match the executor this create names. Stop it first (write cairn:~/repl/x mode delete), or create it without `executor`.".into()),
        )
        .unwrap_err();
        assert!(!running.contains("without `executor`"), "{running}");
        assert!(running.contains("name the machine it runs on"), "{running}");

        // The refusal residency actually writes, so a change to its wording
        // that the rewrite no longer recognises fails here rather than quietly
        // putting the advice that cannot work back.
        let crate::fleet::residency::ResidentPlacement::Refuse(real) =
            crate::fleet::residency::decide_resident_placement(
                "terminal `dev`",
                Some(&named("bbs")),
                Some(&crate::fleet::residency::ResidentHome {
                    executor_name: "mac".into(),
                    satisfies_selector: Some(false),
                    live: vec!["REPL `x`".into()],
                }),
            )
        else {
            panic!("a live process on a refused machine refuses the create");
        };
        let rewritten = resident_outcome(Some(&default), Err(real)).unwrap_err();
        assert!(!rewritten.contains("without `executor`"), "{rewritten}");
        assert!(
            rewritten.contains("with `executor:{name:\"mac\"}` to join it on **mac**"),
            "{rewritten}"
        );

        // Without a default the refusal is the create's own, untouched.
        assert_eq!(
            resident_outcome(
                None,
                Err("x without `executor` to join it on **mac**".into())
            ),
            Err("x without `executor` to join it on **mac**".into())
        );
    }
}
