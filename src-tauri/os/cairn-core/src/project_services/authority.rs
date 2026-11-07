//! What a service's runs may do, and who may call a service.
//!
//! A service holds two kinds of run, both under its home
//! (`cairn://p/<project>/services/<id>`, or for a thread's service
//! `cairn://p/<project>/<thread>/services/<name>`) and both attributed to the
//! service's own Agent principal.
//!
//! A thread's service is callable only by its owning thread: the thread
//! itself, one of its tasks, or one of its pages ([`admit_caller`]). So the
//! thread every call acts for is the owner, and every delegation below is
//! bounded by the owning thread's own authority: a thread cannot mint a
//! service that outranks it.
//!
//! - The **standing** run is the incarnation. Its startup code, timers, and
//!   background loops act through it, and it may only read its own home. A
//!   service cannot do anything in Cairn on its own account.
//! - A **delegated** run is minted for one call and finalized when the call
//!   ends. Its authority is the service's intersected with the caller's, and
//!   every Cairn request the service makes while serving that call carries it.
//!   For an agent caller in the same project that intersection is
//!   representable: project resources, under the service's `deny` fence, with
//!   every `run` batch verdict-only and nothing committed. A page caller's
//!   viewer grants never enter it: a page's call reaches its owning thread's
//!   database and may run code only as a page-call batch, which the run tool
//!   places under the page-call sandbox policy (no checkout, reads confined to
//!   the system, the interpreter, and its own scratch; see
//!   `docs/worktree-fence.md`).
//!
//! The intersection is enforced here, at the one dispatch door every tool call
//! passes (`crate::dispatch::dispatch_tool`), and it is narrower than the
//! caller wherever narrowing was simpler than modelling: no `file:` writes, no
//! service control or service calls (no chains, no loops), no places outside
//! the project, no REPLs or workflow targets. One caller is not narrowed that
//! far: the thread that owns a service, calling it itself (its schedule, its
//! session, its tasks), also invokes its MCP tools, skill scripts, and executor
//! verbs ([`DelegatedScope::Owner`]), each logged, and never a prompt.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::RwLock;

use super::protocol::CallerKind;
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use cairn_uri::CairnResource;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ServiceRunRole {
    Standing,
    Delegated {
        caller_kind: CallerKind,
        acting_for: String,
        scope: DelegatedScope,
    },
}

/// What a call's delegated run may do: the service's authority intersected
/// with its caller's, as far as that intersection can be represented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DelegatedScope {
    /// An agent in the service's own project: project resources, under the
    /// service's fence, with the narrowings [`classify_delegated`] applies,
    /// plus the database of the agent's own thread when it is one.
    ProjectAgent { thread: Option<String> },
    /// A thread's service called by that thread itself: its schedule, its
    /// session, or one of its tasks. Everything [`DelegatedScope::ProjectAgent`]
    /// admits for the thread, plus the `target` items the thread can use from a
    /// turn: MCP tools (a machine service's included) and skill scripts, each under the
    /// target's own gates and each logged. A service call has no turn to answer
    /// a prompt, so one that would prompt is refused instead (tenet 1313's
    /// ruling on cairn/5624).
    Owner { thread: String },
    /// A page's call: the owning thread's database, read and written, plus
    /// verdict-only `command` and `code` batches, which the run tool places
    /// only as page-call batches (scratch-only, read-allowlisted, relay-only,
    /// on an executor that has proven that policy). Nothing else: no files,
    /// no other resource, no tool targets, no commit, branch, or placement
    /// choice ([`classify_page_call`]).
    PageCall { thread: String },
    /// Nothing. The call may still be served; any Cairn request made for it
    /// is refused with `reason`.
    Empty { reason: String },
}

impl DelegatedScope {
    /// The thread whose database a call may write, if any.
    pub(crate) fn writable_thread(&self) -> Option<&str> {
        match self {
            DelegatedScope::ProjectAgent { thread } => thread.as_deref(),
            DelegatedScope::Owner { thread } | DelegatedScope::PageCall { thread } => Some(thread),
            DelegatedScope::Empty { .. } => None,
        }
    }
}

/// The thread a home acts in: the thread itself or one of its tasks.
fn acting_thread(home: &str) -> Option<String> {
    acting_thread_of(home).map(|(_, name)| name)
}

/// [`acting_thread`], with the project the thread is in.
fn acting_thread_of(home: &str) -> Option<(String, String)> {
    match cairn_uri::parse_uri(home)? {
        CairnResource::Thread {
            project,
            name,
            path,
        } if path.is_empty()
            || matches!(path.as_slice(), [task, task_name] if task == "task" && !task_name.is_empty()) =>
        {
            Some((project, name))
        }
        _ => None,
    }
}

/// Whether `caller` may call a service `owner` owns, or why not.
///
/// A project service takes any caller its project's gates let through. A
/// thread's service is its thread's: it serves the thread itself, the
/// thread's tasks, and the thread's pages, and no one else. That is what keeps
/// a call's authority inside the owning thread's: the delegation a call gets
/// ([`delegation_for`]) is computed for the thread the caller acts for, which
/// is then always the owner.
pub(crate) fn admit_caller(
    owner: &super::ServiceOwner,
    project_key: &str,
    caller: &super::protocol::CallerFrame,
) -> Result<(), String> {
    let super::ServiceOwner::Thread { name: owner, .. } = owner else {
        return Ok(());
    };
    match acting_thread_of(&caller.acting_for) {
        Some((project, thread)) if project == project_key && thread == *owner => Ok(()),
        _ => Err(format!(
            "a thread's service serves only its own thread: {}, its tasks, and its pages. \
             This call acts for {}",
            cairn_uri::build_thread_uri(project_key, owner),
            caller.acting_for
        )),
    }
}

/// The authority a call executes under. The one place that decision is made,
/// for every kind of caller, and it has three answers: the owning thread's own
/// call ([`DelegatedScope::Owner`]), another agent's call
/// ([`DelegatedScope::ProjectAgent`]), and a page's call
/// ([`DelegatedScope::PageCall`]).
///
/// The thread a call acts for owns the call's data: a page's service reads
/// and writes the page's thread database, and an agent's service writes the
/// agent's own thread database. That a page's call reaches its owning
/// thread's database without the viewer's grants entering into it is the
/// operator's decision on cairn/5518; that it may also run code, only as a
/// page-call batch, is the operator's ruling on cairn/5709 (tenet 1330).
/// Nothing else a page's call can reach comes from the owner.
pub(crate) fn delegation_for(
    owner: &super::ServiceOwner,
    caller: &super::protocol::CallerFrame,
) -> DelegatedScope {
    let thread = acting_thread(&caller.acting_for);
    match caller.kind {
        // The thread that owns the service, calling it itself (its schedule,
        // its session, its tasks), reaches what it reaches from a turn.
        CallerKind::Agent | CallerKind::Schedule
            if matches!(
                (owner, &thread),
                (super::ServiceOwner::Thread { name, .. }, Some(acting)) if name == acting
            ) =>
        {
            DelegatedScope::Owner {
                thread: thread.unwrap_or_default(),
            }
        }
        // Any other agent, and a project's schedule (which acts for the
        // project and so writes no thread's database), keeps the narrowed
        // agent scope.
        CallerKind::Agent | CallerKind::Schedule => DelegatedScope::ProjectAgent { thread },
        CallerKind::Page => match thread {
            Some(thread) => DelegatedScope::PageCall { thread },
            None => DelegatedScope::Empty {
                reason: "a page's call reaches only its owning thread's database, and this page \
                         is a draft with no owning thread"
                    .to_string(),
            },
        },
    }
}

/// A live service run, as the gate needs to know it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServiceRun {
    pub project_id: String,
    pub project_key: String,
    pub owner: super::ServiceOwner,
    pub service: String,
    pub generation: u64,
    pub default_branch: String,
    pub role: ServiceRunRole,
}

impl ServiceRun {
    pub(crate) fn home(&self) -> String {
        self.owner.home(&self.project_key, &self.service)
    }
}

/// Every live service run this runner supervises, by run id.
///
/// Process-local on purpose, like the bearers the runs authenticate with: a
/// restart drops both, and startup finalizes the rows a predecessor left live.
#[derive(Debug, Default)]
pub(crate) struct ServiceRuns {
    runs: RwLock<HashMap<String, ServiceRun>>,
    /// Runs already shown not to belong to a service. A run's job never
    /// changes, so a confirmation holds for the run's life; the set is a
    /// bound on lookups, not a source of authority.
    ordinary: RwLock<std::collections::HashSet<String>>,
    /// What each live delegated run has written, so the call's logs name it.
    writes: RwLock<HashMap<String, Vec<String>>>,
    /// A prompt a live delegated run would have raised, refused instead. The
    /// call it belongs to fails with it whatever the method answered.
    prompts: RwLock<HashMap<String, String>>,
    /// The `run` batches each live delegated run has in flight, so the call's
    /// end stops them rather than letting them run to their item timeout.
    in_flight: RwLock<HeldBatches>,
    /// Every batch each live delegated run started, so the call's logs name
    /// the code it ran: its request id, and the refusal it settled with when
    /// it never ran.
    started: RwLock<HashMap<String, Vec<(String, Option<String>)>>>,
    /// Runs whose call has begun ending. A batch that reaches `hold_batch`
    /// after this is not held but refused, so the gap between `stop_batches`
    /// and `remove` cannot leak a batch that runs to its own timeout.
    ended: RwLock<std::collections::HashSet<String>>,
}

/// A `run` batch a service run started, as the call's end needs it: a way to
/// stop it. Defined here so `project_services` does not depend on the run
/// handler's internals.
pub(crate) trait BatchStop: Send + Sync {
    /// Interrupt the batch's items and cancel its placement and its fleet
    /// request. A verdict-only batch publishes nothing, so it settles through
    /// the ordinary cancelled outcome.
    fn stop(&self, orch: &Orchestrator);
}

/// The in-flight batches of each live delegated run, by request id.
#[derive(Default)]
struct HeldBatches(HashMap<String, Vec<(String, std::sync::Arc<dyn BatchStop>)>>);

impl std::fmt::Debug for HeldBatches {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.0.iter().map(|(run, batches)| {
                (
                    run,
                    batches
                        .iter()
                        .map(|(request, _)| request)
                        .collect::<Vec<_>>(),
                )
            }))
            .finish()
    }
}

/// Ordinary runs remembered before the set is cleared and relearned.
const ORDINARY_RUNS_REMEMBERED: usize = 50_000;

impl ServiceRuns {
    pub(crate) fn insert(&self, run_id: &str, run: ServiceRun) {
        self.runs
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(run_id.to_string(), run);
    }

    pub(crate) fn remove(&self, run_id: &str) -> Option<ServiceRun> {
        self.ended
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(run_id);
        self.runs
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(run_id)
    }

    pub(crate) fn get(&self, run_id: &str) -> Option<ServiceRun> {
        self.runs
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(run_id)
            .cloned()
    }

    fn note_writes(&self, run_id: &str, targets: Vec<String>) {
        if targets.is_empty() {
            return;
        }
        let mut writes = self
            .writes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = writes.entry(run_id.to_string()).or_default();
        for target in targets {
            if !entry.contains(&target) && entry.len() < 256 {
                entry.push(target);
            }
        }
    }

    /// Record that `run_id` would have raised a prompt, and was refused.
    pub(crate) fn note_prompt_refusal(&self, run_id: &str, refusal: &str) {
        self.prompts
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(run_id.to_string())
            .or_insert_with(|| refusal.to_string());
    }

    /// The first prompt `run_id` was refused, forgotten as it is taken.
    pub(crate) fn take_prompt_refusal(&self, run_id: &str) -> Option<String> {
        self.prompts
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(run_id)
    }

    /// Hold a batch `run_id` started until it settles or the call ends.
    /// Nothing is held for a run the supervisor does not hold: an ordinary
    /// run's batches are not a call's.
    /// Hold a batch until it settles or the call ends, returning whether it
    /// was held. `false` means the call has ended (or was never a held run):
    /// its caller must stop the batch itself rather than leave it running,
    /// because nothing else now will.
    pub(crate) fn hold_batch(
        &self,
        run_id: &str,
        request_id: &str,
        batch: std::sync::Arc<dyn BatchStop>,
    ) -> bool {
        if self.get(run_id).is_none()
            || self
                .ended
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .contains(run_id)
        {
            return false;
        }
        self.in_flight
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .0
            .entry(run_id.to_string())
            .or_default()
            .push((request_id.to_string(), batch));
        let mut started = self
            .started
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = started.entry(run_id.to_string()).or_default();
        if entry.len() < 64 {
            entry.push((request_id.to_string(), None));
        }
        true
    }

    /// Let go of a batch that settled on its own, keeping the refusal it
    /// settled with when it never ran, so the call's logs can say why in the
    /// words the fleet used.
    pub(crate) fn release_batch(&self, run_id: &str, request_id: &str, refusal: Option<String>) {
        if let Some(refusal) = refusal {
            if let Some(entry) = self
                .started
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get_mut(run_id)
                .and_then(|batches| batches.iter_mut().find(|(held, _)| held == request_id))
            {
                entry.1 = Some(refusal);
            }
        }
        let mut in_flight = self
            .in_flight
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(batches) = in_flight.0.get_mut(run_id) {
            batches.retain(|(held, _)| held != request_id);
            if batches.is_empty() {
                in_flight.0.remove(run_id);
            }
        }
    }

    /// Stop every batch `run_id` still has in flight. The handles are taken
    /// out under the lock and stopped after it is dropped, because stopping
    /// takes the fleet's locks and may emit events.
    pub(crate) fn stop_batches(&self, orch: &Orchestrator, run_id: &str) -> usize {
        // Mark the call ended first, so a batch that reaches `hold_batch`
        // between here and `remove` is refused rather than held unstopped.
        self.ended
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(run_id.to_string());
        let batches = self
            .in_flight
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .0
            .remove(run_id)
            .unwrap_or_default();
        // Record why each stopped batch did not run, before its own task's
        // `release_batch` would: the call's log is taken (`take_batches`)
        // right after this, and a batch the call cancelled must read as
        // stopped, never as "ran".
        if !batches.is_empty() {
            let mut started = self
                .started
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(entry) = started.get_mut(run_id) {
                for (request_id, _) in &batches {
                    if let Some(held) = entry.iter_mut().find(|(held, _)| held == request_id) {
                        held.1
                            .get_or_insert_with(|| "the call ended before it finished".to_string());
                    }
                }
            }
        }
        for (_, batch) in &batches {
            batch.stop(orch);
        }
        batches.len()
    }

    /// Every batch `run_id` started, with the refusal of each that never ran,
    /// forgotten as it is taken.
    pub(crate) fn take_batches(&self, run_id: &str) -> Vec<(String, Option<String>)> {
        self.started
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(run_id)
            .unwrap_or_default()
    }

    /// What `run_id` wrote, forgotten as it is taken.
    pub(crate) fn take_writes(&self, run_id: &str) -> Vec<String> {
        self.writes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(run_id)
            .unwrap_or_default()
    }

    fn known_ordinary(&self, run_id: &str) -> bool {
        self.ordinary
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(run_id)
    }

    fn remember_ordinary(&self, run_id: &str) {
        let mut ordinary = self
            .ordinary
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if ordinary.len() >= ORDINARY_RUNS_REMEMBERED {
            ordinary.clear();
        }
        ordinary.insert(run_id.to_string());
    }

    pub(crate) fn for_request(&self, request: &McpCallbackRequest) -> Option<ServiceRun> {
        request
            .run_id
            .as_deref()
            .filter(|run_id| !run_id.is_empty())
            .and_then(|run_id| self.get(run_id))
    }
}

/// The refusal for a tool call from a service run, or `None` when it may
/// proceed (including every call that is not from a service run at all).
/// A refusal is also written to the service's log.
pub(crate) async fn refuse(orch: &Orchestrator, request: &McpCallbackRequest) -> Option<String> {
    if let Some(run) = orch.project_services.runs.for_request(request) {
        match classify(&run, &request.tool, &request.payload) {
            Ok(()) => {
                if request.tool == "write" {
                    let run_id = request.run_id.as_deref().unwrap_or_default();
                    orch.project_services
                        .runs
                        .note_writes(run_id, write_targets(&run, &request.payload));
                }
                if request.tool == "run" {
                    for target in run_targets(&request.payload) {
                        super::note_target(orch, &run, &target).await;
                    }
                }
                return None;
            }
            Err(refusal) => {
                super::note_refusal(orch, &run, &request.tool, &refusal).await;
                return Some(refusal);
            }
        }
    }
    // Not a run the supervisor holds. That is every ordinary agent run, and
    // also a service run the supervisor has let go of (a call that ended, an
    // incarnation from before a restart). The second must never be served as
    // the first, so the run's job decides, and an answer that cannot be
    // read refuses.
    let run_id = request.run_id.as_deref().filter(|run| !run.is_empty())?;
    let runs = &orch.project_services.runs;
    if runs.known_ordinary(run_id) {
        return None;
    }
    match run_belongs_to_a_service(orch, run_id).await {
        Ok(false) => {
            runs.remember_ordinary(run_id);
            None
        }
        Ok(true) => Some(format!(
            "run {run_id} belongs to a project service and is no longer live; a service's \
             requests are served only while its supervisor holds the run"
        )),
        Err(error) => Some(format!(
            "run {run_id} could not be confirmed as an ordinary run ({error}); refused"
        )),
    }
}

/// The absolute URIs an admitted write names, for the call's provenance.
fn write_targets(run: &ServiceRun, payload: &Value) -> Vec<String> {
    payload
        .get("changes")
        .and_then(Value::as_array)
        .map(|changes| {
            changes
                .iter()
                .filter_map(|change| change.get("target").and_then(Value::as_str))
                .filter_map(|target| {
                    project_resource(run, target).map(|resource| resource.to_uri())
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Whether `run_id`'s job is a service job. A run no database knows is not
/// one; the ownership fences answer for unknown runs.
async fn run_belongs_to_a_service(orch: &Orchestrator, run_id: &str) -> Result<bool, String> {
    let Ok(db) = crate::execution::routing::routing_db_for_id(&orch.db, run_id).await else {
        return Ok(false);
    };
    let config = db
        .query_opt_text(
            "SELECT j.agent_config_id FROM runs r JOIN jobs j ON j.id = r.job_id WHERE r.id = ?1",
            cairn_db::turso::params![run_id],
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(config.as_deref() == Some(super::SERVICE_AGENT_CONFIG))
}

fn without_query(target: &str) -> &str {
    target.split(['?', '#']).next().unwrap_or(target)
}

fn is_at_or_under(target: &str, root: &str) -> bool {
    target == root
        || target
            .strip_prefix(root)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn string_list<'a>(payload: &'a Value, key: &str) -> Vec<&'a str> {
    match payload.get(key) {
        Some(Value::String(one)) => vec![one.as_str()],
        Some(Value::Array(items)) => items.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    }
}

/// Whether `run` may make the tool call `tool` with `payload`.
pub(crate) fn classify(run: &ServiceRun, tool: &str, payload: &Value) -> Result<(), String> {
    match &run.role {
        ServiceRunRole::Standing => classify_standing(run, tool, payload),
        ServiceRunRole::Delegated {
            scope: DelegatedScope::Empty { reason },
            ..
        } => Err(format!(
            "service `{}` cannot `{tool}` for this call: {reason}",
            run.service
        )),
        ServiceRunRole::Delegated {
            scope: DelegatedScope::ProjectAgent { thread },
            ..
        } => classify_delegated(run, thread.as_deref(), false, tool, payload),
        ServiceRunRole::Delegated {
            scope: DelegatedScope::Owner { thread },
            ..
        } => classify_delegated(run, Some(thread), true, tool, payload),
        ServiceRunRole::Delegated {
            scope: DelegatedScope::PageCall { thread },
            ..
        } => classify_page_call(run, thread, tool, payload),
    }
}

impl ServiceRun {
    /// Whether this run is a page's call, whose `run` batches the run tool
    /// must place only as page-call batches.
    pub(crate) fn is_page_call(&self) -> bool {
        matches!(
            self.role,
            ServiceRunRole::Delegated {
                scope: DelegatedScope::PageCall { .. },
                ..
            }
        )
    }
}

/// Whether `resource` is `thread`'s database or something under it.
fn in_thread_db<'a>(resource: &'a CairnResource, thread: &str) -> Option<&'a [String]> {
    match resource {
        CairnResource::Thread { name, path, .. }
            if name == thread && path.first().map(String::as_str) == Some("db") =>
        {
            Some(&path[1..])
        }
        _ => None,
    }
}

/// A write to `thread`'s database: SQL on the database itself, or rows of a
/// table (never the reserved `schema`).
fn thread_db_writable(resource: &CairnResource, thread: &str, mode: &str) -> bool {
    match in_thread_db(resource, thread) {
        Some([]) => mode == "patch",
        Some([table, rest @ ..]) if table != "schema" && rest.len() <= 1 => {
            matches!(mode, "create" | "append" | "patch" | "replace" | "delete")
        }
        _ => false,
    }
}

/// The interpreters a page-call batch may name. Each must be one the executor
/// has proven runs under the page-call policy, which placement checks.
pub(crate) const PAGE_CALL_INTERPRETERS: [&str; 6] =
    ["python", "py", "typescript", "ts", "javascript", "js"];

fn classify_page_call(
    run: &ServiceRun,
    thread: &str,
    tool: &str,
    payload: &Value,
) -> Result<(), String> {
    let root = format!(
        "{}/db",
        cairn_uri::build_thread_uri(&run.project_key, thread)
    );
    let refused = |what: String| {
        format!(
            "service `{}` serving a page may not {what}: a page's call reads and writes only its \
             owning thread's database ({root}) and runs code only as a verdict-only page-call batch",
            run.service
        )
    };
    match tool {
        "run" => classify_run(RunCaller::Page, payload, &refused),
        "read_batch" => {
            let paths = string_list(payload, "paths");
            if paths.is_empty() {
                return Err(refused("read without naming targets".to_string()));
            }
            for path in paths {
                let inside = project_resource(run, path)
                    .is_some_and(|resource| in_thread_db(&resource, thread).is_some());
                if !inside {
                    return Err(refused(format!("read `{path}`")));
                }
            }
            Ok(())
        }
        "write" => {
            let changes = payload
                .get("changes")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if changes.is_empty() {
                return Err(refused("write without naming targets".to_string()));
            }
            for change in &changes {
                let target = change.get("target").and_then(Value::as_str).unwrap_or("");
                let mode = change.get("mode").and_then(Value::as_str).unwrap_or("");
                let allowed = project_resource(run, target)
                    .is_some_and(|resource| thread_db_writable(&resource, thread, mode));
                if !allowed {
                    return Err(refused(format!("`{mode}` `{target}`")));
                }
            }
            Ok(())
        }
        other => Err(refused(format!("use `{other}`"))),
    }
}

fn classify_standing(run: &ServiceRun, tool: &str, payload: &Value) -> Result<(), String> {
    let home = run.home();
    let outside = |what: String| {
        format!(
            "service `{}` acting on its own account may only read its home ({home}/**); {what}. \
             Anything else happens inside a call, under that call's delegated authority (ctx.cairn).",
            run.service
        )
    };
    if tool != "read_batch" {
        return Err(outside(format!("`{tool}` is not a read of it")));
    }
    let paths = string_list(payload, "paths");
    if paths.is_empty() {
        return Err(outside("the read named no targets".to_string()));
    }
    for path in paths {
        // Judged on what the target resolves to, not how it is spelled: a
        // home-relative suffix can expand outside the home (attachments
        // resolve to the project).
        let target = without_query(path);
        let resolved = crate::resources::resolve_home_relative_against(&home, target)
            .unwrap_or_else(|| target.to_string());
        if !is_at_or_under(&without_query(&resolved).to_ascii_lowercase(), &home) {
            return Err(outside(format!("`{path}` is outside it")));
        }
    }
    Ok(())
}

/// A delegated target as a parsed resource in the run's own project, with
/// `cairn:@/` expanded. `None` for anything else.
fn project_resource(run: &ServiceRun, target: &str) -> Option<CairnResource> {
    let target = without_query(target);
    let absolute = if target == "cairn:@" {
        cairn_uri::build_project_uri(&run.project_key)
    } else if let Some(rest) = target.strip_prefix("cairn:@/") {
        format!("{}/{rest}", cairn_uri::build_project_uri(&run.project_key))
    } else {
        target.to_string()
    };
    let resource = cairn_uri::parse_uri(&absolute)?;
    (resource.project_key() == Some(run.project_key.as_str())).then_some(resource)
}

/// The records a call may write: conversation and thread-database rows,
/// nothing that starts a process, an agent, or a merge, and nothing that
/// outlives the call as an actor.
fn writable(resource: &CairnResource, mode: &str) -> bool {
    match resource {
        CairnResource::Issue { .. } | CairnResource::IssueComments { .. } => mode == "append",
        CairnResource::IssueMessages { .. }
        | CairnResource::ProjectMessages { .. }
        | CairnResource::NodeMessages { .. } => mode == "append",
        // The parser leaves a thread's sub-path for the thread resolver, so a
        // thread's messages arrive as a path segment. A thread's database is
        // writable only by the call acting for that thread (see
        // `thread_db_writable` and the store's ownership rule).
        CairnResource::Thread { path, .. } => {
            mode == "append" && matches!(path.as_slice(), [messages] if messages == "messages")
        }
        _ => false,
    }
}

/// Whether `target` names something an owner-scoped call may invoke as a
/// `run` item: an MCP tool or a skill script.
fn is_owner_target(target: &str) -> bool {
    let target = without_query(target);
    let parts = |prefix: &str| -> Option<Vec<String>> {
        target
            .strip_prefix(prefix)
            .map(|rest| rest.split('/').map(str::to_string).collect())
    };
    let named = |segments: &[String]| segments.iter().all(|segment| !segment.is_empty());
    match (parts("cairn://mcp/"), parts("cairn://skills/")) {
        (Some(mcp), _) => mcp.len() == 2 && named(&mcp),
        (_, Some(skill)) => skill.len() == 3 && skill[1] == "scripts" && named(&skill),
        _ => false,
    }
}

/// The `target` items of a `run` payload, for the call's log.
pub(crate) fn run_targets(payload: &Value) -> Vec<String> {
    payload
        .get("commands")
        .and_then(Value::as_array)
        .map(|commands| {
            commands
                .iter()
                .filter_map(|item| item.get("target").and_then(Value::as_str))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn classify_delegated(
    run: &ServiceRun,
    own_thread: Option<&str>,
    owner_targets: bool,
    tool: &str,
    payload: &Value,
) -> Result<(), String> {
    let refused = |what: String| {
        format!(
            "service `{}` serving a call may not {what}: a call executes as the service \
             intersected with its caller, inside project {}",
            run.service, run.project_key
        )
    };
    match tool {
        "read_batch" => {
            let paths = string_list(payload, "paths");
            if paths.is_empty() {
                return Err(refused("read without naming targets".to_string()));
            }
            for path in paths {
                let target = without_query(path);
                // A call reads the project's records and the public web; never
                // files, which resolve against a cwd the caller supplies.
                // The owning thread may also read what it may invoke: an MCP
                // server's tools (a machine service's per machine) and a
                // skill's scripts.
                let allowed = target.starts_with("https://")
                    || target.starts_with("http://")
                    || project_resource(run, target).is_some()
                    || (owner_targets
                        && ["cairn://mcp", "cairn://skills"]
                            .iter()
                            .any(|root| is_at_or_under(target, root)));
                if !allowed {
                    return Err(refused(format!("read `{path}`")));
                }
            }
            Ok(())
        }
        "write" => {
            let changes = payload
                .get("changes")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if changes.is_empty() {
                return Err(refused("write without naming targets".to_string()));
            }
            for change in &changes {
                let target = change.get("target").and_then(Value::as_str).unwrap_or("");
                let mode = change.get("mode").and_then(Value::as_str).unwrap_or("");
                let allowed = project_resource(run, target).is_some_and(|resource| {
                    writable(&resource, mode)
                        || own_thread
                            .is_some_and(|thread| thread_db_writable(&resource, thread, mode))
                });
                if !allowed {
                    return Err(refused(format!(
                        "`{mode}` `{target}`: a call writes only comments, messages, and the calling thread's database"
                    )));
                }
            }
            Ok(())
        }
        "run" => classify_run(
            if owner_targets {
                RunCaller::Owner
            } else {
                RunCaller::Agent
            },
            payload,
            &refused,
        ),
        other => Err(refused(format!("use `{other}`"))),
    }
}

/// Which delegated caller a `run` payload is judged for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunCaller {
    /// Another agent's call: `command` and `code` items.
    Agent,
    /// The owning thread's own call: also its MCP tools and skill scripts.
    Owner,
    /// A page's call: `command` and `code` items only, with nothing that
    /// chooses where or against what they run, since placement does that.
    Page,
}

/// The batch keys a page's `run` may carry. Anything else (a commit, a
/// branch, an executor pin, a conflict-marker reason, a key this list has
/// not met) is refused rather than ignored.
const PAGE_RUN_KEYS: [&str; 3] = ["commands", "sequential", "stop_on_error"];
/// The item keys a page's `run` item may carry.
const PAGE_ITEM_KEYS: [&str; 5] = ["command", "code", "interpreter", "timeout", "description"];

/// The one `run` grammar for a delegated call. Every caller's batch is
/// verdict-only; the callers differ only in what items they may name.
fn classify_run(
    caller: RunCaller,
    payload: &Value,
    refused: &dyn Fn(String) -> String,
) -> Result<(), String> {
    let present = |value: &Value, key: &str| value.get(key).is_some_and(|value| !value.is_null());
    if present(payload, "commit_msg") || present(payload, "commitMsg") {
        return Err(refused(
            "commit: a service's run batches are verdict-only".to_string(),
        ));
    }
    if caller == RunCaller::Page {
        if let Some(fields) = payload.as_object() {
            for (key, value) in fields {
                if !value.is_null() && !PAGE_RUN_KEYS.contains(&key.as_str()) {
                    return Err(refused(format!(
                        "run a batch with `{key}`: where a page's batch runs, and against what, \
                         is the page-call placement rule's to decide"
                    )));
                }
            }
        }
    }
    let commands = payload
        .get("commands")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for item in &commands {
        for key in ["repl", "waitFor", "wait_for"] {
            if present(item, key) {
                return Err(refused(format!(
                    "run an item with `{key}`: only `command`, `code`, and the owning \
                     thread's tool targets run for a call"
                )));
            }
        }
        if let Some(target) = item.get("target").and_then(Value::as_str) {
            if caller != RunCaller::Owner {
                return Err(refused(format!(
                    "run `{target}`: only `command` and `code` items run for a call that \
                     is not the owning thread's own"
                )));
            }
            if !is_owner_target(target) {
                return Err(refused(format!(
                    "run `{target}`: a call may invoke an MCP tool \
                     (cairn://mcp/<server>/<tool>, a machine service's on the machine its \
                     batch's executor names) or a skill script \
                     (cairn://skills/<id>/scripts/<name>), and nothing else"
                )));
            }
        }
        if caller == RunCaller::Page {
            classify_page_item(item, refused)?;
        }
    }
    Ok(())
}

fn classify_page_item(item: &Value, refused: &dyn Fn(String) -> String) -> Result<(), String> {
    let Some(fields) = item.as_object() else {
        return Err(refused("run an item that is not an object".to_string()));
    };
    for (key, value) in fields {
        if !value.is_null() && !PAGE_ITEM_KEYS.contains(&key.as_str()) {
            return Err(refused(format!(
                "run an item with `{key}`: a page's batch runs only `command` and `code` items"
            )));
        }
    }
    let has = |key: &str| fields.get(key).is_some_and(|value| !value.is_null());
    if !has("command") && !has("code") {
        return Err(refused(
            "run an item with neither `command` nor `code`".to_string(),
        ));
    }
    if has("code") {
        let interpreter = fields
            .get("interpreter")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if !PAGE_CALL_INTERPRETERS.contains(&interpreter.as_str()) {
            return Err(refused(format!(
                "run `{interpreter}` code: a page's batch runs python, typescript, or javascript"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run(role: ServiceRunRole) -> ServiceRun {
        ServiceRun {
            project_id: "p".into(),
            project_key: "cairn".into(),
            owner: crate::project_services::ServiceOwner::Project,
            service: "judge".into(),
            generation: 1,
            default_branch: "main".into(),
            role,
        }
    }

    fn spanish() -> crate::project_services::ServiceOwner {
        crate::project_services::ServiceOwner::Thread {
            id: "t-1".into(),
            name: "spanish".into(),
        }
    }

    /// A thread's service serves its thread, the thread's tasks, and the
    /// thread's pages; no other thread, issue node, or project.
    #[test]
    fn a_thread_service_admits_only_callers_acting_for_its_thread() {
        let owner = spanish();
        for acting_for in [
            "cairn://p/cairn/spanish",
            "cairn://p/cairn/spanish/task/drill",
        ] {
            for kind in [CallerKind::Agent, CallerKind::Page] {
                assert!(
                    admit_caller(&owner, "cairn", &caller(kind, acting_for)).is_ok(),
                    "{acting_for}"
                );
            }
        }
        for acting_for in [
            "cairn://p/cairn/french",
            "cairn://p/cairn/french/task/spanish",
            "cairn://p/cairn/12/1/builder",
            "cairn://p/other/spanish",
            "cairn://p/cairn/spanish/services/tutor",
            "not a uri",
        ] {
            let refusal =
                admit_caller(&owner, "cairn", &caller(CallerKind::Agent, acting_for)).unwrap_err();
            assert!(refusal.contains("cairn://p/cairn/spanish"), "{refusal}");
        }
        // A project service is not the owner's to gate.
        assert!(admit_caller(
            &crate::project_services::ServiceOwner::Project,
            "cairn",
            &caller(CallerKind::Agent, "cairn://p/cairn/12/1/builder")
        )
        .is_ok());
    }

    /// Every call an admitted caller makes acts for the owning thread, so the
    /// one database a call reaches is the owner's.
    #[test]
    fn an_admitted_call_writes_only_the_owning_threads_database() {
        for kind in [CallerKind::Agent, CallerKind::Page, CallerKind::Schedule] {
            let frame = caller(kind, "cairn://p/cairn/spanish/task/drill");
            assert!(admit_caller(&spanish(), "cairn", &frame).is_ok());
            assert_eq!(
                delegation_for(&spanish(), &frame).writable_thread(),
                Some("spanish")
            );
        }
    }

    /// A thread service's standing run reads only its own home, which sits
    /// beneath its thread.
    #[test]
    fn a_thread_services_standing_run_reaches_only_its_home() {
        let mut standing = run(ServiceRunRole::Standing);
        standing.owner = spanish();
        standing.service = "tutor".into();
        assert_eq!(standing.home(), "cairn://p/cairn/spanish/services/tutor");
        let read = |path: &str| classify(&standing, "read_batch", &json!({"paths": [path]}));
        assert!(read("cairn:~/status").is_ok());
        assert!(read("cairn://p/cairn/spanish/services/tutor/log").is_ok());
        for outside in [
            "cairn://p/cairn/spanish",
            "cairn://p/cairn/spanish/db?sql=SELECT 1",
            "cairn://p/cairn/spanish/services/other",
            "cairn://p/cairn/services/tutor",
        ] {
            assert!(read(outside).is_err(), "{outside}");
        }
        assert!(classify(
            &standing,
            "write",
            &json!({"changes": [{"target": "cairn://p/cairn/spanish/db/rows", "mode": "create"}]})
        )
        .is_err());
    }

    fn delegated() -> ServiceRun {
        run(ServiceRunRole::Delegated {
            caller_kind: CallerKind::Agent,
            acting_for: "cairn://p/cairn/12/1/builder".into(),
            scope: DelegatedScope::ProjectAgent { thread: None },
        })
    }

    fn caller(kind: CallerKind, acting_for: &str) -> super::super::protocol::CallerFrame {
        super::super::protocol::CallerFrame {
            kind,
            acting_for: acting_for.into(),
            agent: None,
            viewer: None,
            instrument: None,
        }
    }

    #[test]
    fn the_thread_a_call_acts_for_owns_its_data() {
        let project = crate::project_services::ServiceOwner::Project;
        assert_eq!(
            delegation_for(&project, &caller(CallerKind::Page, "cairn://p/cairn/prep")),
            DelegatedScope::PageCall {
                thread: "prep".into()
            }
        );
        assert_eq!(
            delegation_for(
                &project,
                &caller(CallerKind::Agent, "cairn://p/cairn/prep/task/scout")
            ),
            DelegatedScope::ProjectAgent {
                thread: Some("prep".into())
            }
        );
        assert_eq!(
            delegation_for(
                &project,
                &caller(CallerKind::Agent, "cairn://p/cairn/12/1/builder")
            ),
            DelegatedScope::ProjectAgent { thread: None }
        );
        assert_eq!(
            delegation_for(&project, &caller(CallerKind::Schedule, "cairn://p/cairn")),
            DelegatedScope::ProjectAgent { thread: None },
            "a project's schedule keeps the project scope"
        );
        assert!(matches!(
            delegation_for(
                &project,
                &caller(CallerKind::Page, "cairn://p/cairn/12/1/builder")
            ),
            DelegatedScope::Empty { .. }
        ));
    }

    /// A thread's own calls to its own service (schedule, session, task) get
    /// the owner scope; its pages do not.
    #[test]
    fn the_owning_thread_calling_its_service_gets_the_owner_scope() {
        for (kind, acting_for) in [
            (CallerKind::Schedule, "cairn://p/cairn/spanish"),
            (CallerKind::Agent, "cairn://p/cairn/spanish"),
            (CallerKind::Agent, "cairn://p/cairn/spanish/task/drill"),
        ] {
            assert_eq!(
                delegation_for(&spanish(), &caller(kind, acting_for)),
                DelegatedScope::Owner {
                    thread: "spanish".into()
                },
                "{kind:?} {acting_for}"
            );
        }
        assert_eq!(
            delegation_for(
                &spanish(),
                &caller(CallerKind::Page, "cairn://p/cairn/spanish")
            ),
            DelegatedScope::PageCall {
                thread: "spanish".into()
            }
        );
    }

    /// The owner scope invokes tool targets; the narrowed agent scope and a
    /// page's do not; nothing invokes a workflow, a response, or a REPL.
    #[test]
    fn only_the_owner_scope_runs_tool_targets() {
        let delegated_as = |scope: DelegatedScope| {
            let mut run = run(ServiceRunRole::Delegated {
                caller_kind: CallerKind::Schedule,
                acting_for: "cairn://p/cairn/spanish".into(),
                scope,
            });
            run.owner = spanish();
            run
        };
        let owner = delegated_as(DelegatedScope::Owner {
            thread: "spanish".into(),
        });
        let agent = delegated_as(DelegatedScope::ProjectAgent {
            thread: Some("spanish".into()),
        });
        let page = delegated_as(DelegatedScope::PageCall {
            thread: "spanish".into(),
        });
        let target = |target: &str| json!({"commands": [{"target": target}]});
        for allowed in [
            "cairn://mcp/cairn-bugs/list_reports",
            "cairn://mcp/axon/look",
            "cairn://skills/testing/scripts/run-coverage",
        ] {
            assert!(
                classify(&owner, "run", &target(allowed)).is_ok(),
                "{allowed}"
            );
            assert!(
                classify(&agent, "run", &target(allowed)).is_err(),
                "{allowed}"
            );
            assert!(
                classify(&page, "run", &target(allowed)).is_err(),
                "{allowed}"
            );
        }
        for refused in [
            "cairn://workflows/release",
            "cairn://responses/summarize",
            "cairn://mcp/cairn-bugs",
            "cairn://executors/local",
            "cairn://executors/local/look",
            "cairn://p/cairn/spanish/procedures/collect",
        ] {
            assert!(
                classify(&owner, "run", &target(refused)).is_err(),
                "{refused}"
            );
        }
        assert!(classify(
            &owner,
            "run",
            &json!({"commands": [{"repl": "py", "code": "1"}]})
        )
        .is_err());
        assert!(classify(
            &owner,
            "read_batch",
            &json!({"paths": ["cairn://mcp/cairn-bugs"]})
        )
        .is_ok());
        assert!(classify(
            &agent,
            "read_batch",
            &json!({"paths": ["cairn://mcp/cairn-bugs"]})
        )
        .is_err());
        assert_eq!(
            run_targets(&json!({"commands": [{"code": "1"}, {"target": "cairn://mcp/axon/look"}]})),
            vec!["cairn://mcp/axon/look".to_string()]
        );
        // The batch's executor chooses which machine serves a machine-service
        // call, so an owner's call may carry one.
        assert!(classify(
            &owner,
            "run",
            &json!({"executor": {"name": "local"}, "commands": [{"target": "cairn://mcp/axon/look"}]})
        )
        .is_ok());
    }

    fn page_call() -> ServiceRun {
        run(ServiceRunRole::Delegated {
            caller_kind: CallerKind::Page,
            acting_for: "cairn://p/cairn/prep".into(),
            scope: DelegatedScope::PageCall {
                thread: "prep".into(),
            },
        })
    }

    #[test]
    fn a_page_call_reads_and_writes_its_owning_threads_database_and_nothing_else() {
        let page = page_call();
        assert!(page.is_page_call());
        assert!(!delegated().is_page_call());
        assert!(classify(
            &page,
            "read_batch",
            &json!({"paths": ["cairn://p/cairn/prep/db?sql=SELECT 1", "cairn://p/cairn/prep/db/attempts/7"]})
        )
        .is_ok());
        for (target, mode) in [
            ("cairn://p/cairn/prep/db", "patch"),
            ("cairn://p/cairn/prep/db/attempts", "create"),
            ("cairn://p/cairn/prep/db/attempts/7", "patch"),
            ("cairn:@/prep/db/attempts/7", "delete"),
        ] {
            assert!(
                classify(
                    &page,
                    "write",
                    &json!({"changes": [{"target": target, "mode": mode}]})
                )
                .is_ok(),
                "{mode} {target}"
            );
        }
        for (target, mode) in [
            ("cairn://p/cairn/other/db/attempts", "create"),
            ("cairn://p/cairn/prep/db/schema", "create"),
            ("cairn://p/cairn/prep/messages", "append"),
            ("cairn://p/cairn/12", "append"),
        ] {
            assert!(
                classify(
                    &page,
                    "write",
                    &json!({"changes": [{"target": target, "mode": mode}]})
                )
                .is_err(),
                "{mode} {target}"
            );
        }
        for (tool, payload) in [
            ("read_batch", json!({"paths": ["cairn:@/issues"]})),
            (
                "read_batch",
                json!({"paths": ["cairn://p/cairn/other/db?sql=SELECT 1"]}),
            ),
            ("read_batch", json!({"paths": ["file:src/lib.rs"]})),
            ("read_batch", json!({"paths": ["https://example.com"]})),
            ("read_batch", json!({"paths": ["cairn://p/cairn/12"]})),
            ("read_web", json!({})),
            ("check_run", json!({})),
        ] {
            let refusal = classify(&page, tool, &payload).unwrap_err();
            assert_page_bound(&refusal);
        }
    }

    #[derive(Default)]
    struct CountingStop(std::sync::atomic::AtomicUsize);

    impl BatchStop for CountingStop {
        fn stop(&self, _orch: &Orchestrator) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// A delegated run's batches are held until they settle or the call ends;
    /// an ordinary run's are never held.
    #[tokio::test]
    async fn a_call_holds_its_batches_and_its_end_stops_what_is_in_flight() {
        let orch = crate::mcp::handlers::permission::tests::test_orchestrator().await;
        let runs = ServiceRuns::default();
        runs.insert("run-1", page_call());
        let settled = std::sync::Arc::new(CountingStop::default());
        let running = std::sync::Arc::new(CountingStop::default());
        assert!(runs.hold_batch("run-1", "settled", settled.clone()));
        assert!(runs.hold_batch("run-1", "running", running.clone()));
        // An ordinary (non-service) run holds nothing: its caller is told so.
        assert!(!runs.hold_batch("ordinary-run", "ignored", running.clone()));
        // One batch settled on its own, with a placement refusal.
        runs.release_batch(
            "run-1",
            "settled",
            Some(
                "This run could not execute: local: containment: PageCallFilesystemRead unmeasured"
                    .into(),
            ),
        );

        assert_eq!(runs.stop_batches(&orch, "run-1"), 1);
        assert_eq!(running.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(settled.0.load(std::sync::atomic::Ordering::SeqCst), 0);
        // The call has ended: a batch that reaches hold_batch now is refused.
        assert!(!runs.hold_batch("run-1", "late", running.clone()));
        assert_eq!(runs.stop_batches(&orch, "run-1"), 0, "stopped once");
        // The settled batch keeps its own refusal; the stopped one is logged
        // as stopped, never as "ran".
        assert_eq!(
            runs.take_batches("run-1"),
            vec![
                (
                    "settled".to_string(),
                    Some("This run could not execute: local: containment: PageCallFilesystemRead unmeasured".to_string())
                ),
                (
                    "running".to_string(),
                    Some("the call ended before it finished".to_string())
                ),
            ]
        );
        assert!(runs.take_batches("run-1").is_empty());
    }

    fn assert_page_bound(refusal: &str) {
        assert!(
            refusal.starts_with("service `judge` serving a page may not ")
                && refusal.contains("owning thread's database (cairn://p/cairn/prep/db)")
                && refusal.contains("verdict-only page-call batch")
                && !refusal.contains("5564"),
            "{refusal}"
        );
    }

    /// A page's call runs `command` and `code` items, and nothing that
    /// commits, picks a branch or a machine, invokes a tool, or holds a turn.
    #[test]
    fn a_page_call_runs_only_verdict_only_command_and_code_items() {
        let page = page_call();
        for admitted in [
            json!({"commands": [{"code": "print(1)", "interpreter": "python", "timeout": 20000}]}),
            json!({"commands": [{"code": "console.log(1)", "interpreter": "typescript", "description": "tests"}]}),
            json!({"commands": [{"command": "echo hi"}], "sequential": true, "stop_on_error": false}),
            json!({"commands": [{"command": "true", "timeout": null, "code": null}]}),
        ] {
            assert!(classify(&page, "run", &admitted).is_ok(), "{admitted}");
        }
        let refused = [
            json!({"commands": [{"command": "true"}], "commit_msg": "x"}),
            json!({"commands": [{"command": "true"}], "commitMsg": "x"}),
            json!({"commands": [{"command": "true"}], "branch": "main"}),
            json!({"commands": [{"command": "true"}], "executor": {"name": "local"}}),
            json!({"commands": [{"command": "true"}], "conflict_markers_reason": "x"}),
            json!({"commands": [{"target": "cairn://mcp/cairn-bugs/list_reports"}]}),
            json!({"commands": [{"target": "cairn://mcp/axon/look"}]}),
            json!({"commands": [{"target": "cairn://skills/testing/scripts/run-coverage"}]}),
            json!({"commands": [{"target": "cairn://workflows/release", "background": true}]}),
            json!({"commands": [{"code": "1", "interpreter": "python", "repl": "py"}]}),
            json!({"commands": [{"waitFor": {"duration": "1m"}}]}),
            json!({"commands": [{"command": "true", "background": true}]}),
            json!({"commands": [{"command": "true", "payload": {"args": []}}]}),
            json!({"commands": [{"description": "nothing to run"}]}),
            json!({"commands": [{"code": "disp(1)", "interpreter": "matlab"}]}),
            json!({"commands": [{"code": "1"}]}),
            json!({"commands": ["echo hi"]}),
        ];
        for payload in refused {
            let refusal = classify(&page, "run", &payload).expect_err(&payload.to_string());
            assert_page_bound(&refusal);
        }
    }

    #[test]
    fn an_agent_call_writes_its_own_threads_database_only() {
        let from_thread = run(ServiceRunRole::Delegated {
            caller_kind: CallerKind::Agent,
            acting_for: "cairn://p/cairn/prep".into(),
            scope: DelegatedScope::ProjectAgent {
                thread: Some("prep".into()),
            },
        });
        let write = |run: &ServiceRun, target: &str| {
            classify(
                run,
                "write",
                &json!({"changes": [{"target": target, "mode": "create"}]}),
            )
        };
        assert!(write(&from_thread, "cairn://p/cairn/prep/db/attempts").is_ok());
        assert!(write(&from_thread, "cairn://p/cairn/other/db/attempts").is_err());
        assert!(write(&delegated(), "cairn://p/cairn/prep/db/attempts").is_err());
    }

    #[test]
    fn standing_authority_reaches_only_its_home() {
        let standing = run(ServiceRunRole::Standing);
        assert!(classify(
            &standing,
            "read_batch",
            &json!({"paths": ["cairn://p/cairn/services/judge/log?limit=5", "cairn:~/status"]})
        )
        .is_ok());
        for outside in [
            "cairn://p/cairn/12",
            "cairn://p/cairn/services/judgement",
            "cairn://p/cairn/services/echo",
            "cairn:@/issues",
            "file:src/lib.rs",
            "https://example.com",
            // Home-relative, but attachments resolve to the project.
            "cairn:~/attachments",
        ] {
            let refusal =
                classify(&standing, "read_batch", &json!({"paths": [outside]})).expect_err(outside);
            assert!(refusal.contains("inside a call"), "{refusal}");
        }
        for tool in ["write", "run", "read", "read_web", "watch"] {
            assert!(classify(&standing, tool, &json!({})).is_err(), "{tool}");
        }
    }

    #[test]
    fn delegated_reads_stay_inside_the_project_and_off_the_filesystem() {
        let call = delegated();
        assert!(classify(
            &call,
            "read_batch",
            &json!({"paths": ["cairn:@/issues", "cairn://p/cairn/12", "https://example.com"]})
        )
        .is_ok());
        for outside in [
            "cairn://p/other/1",
            "cairn://mcp/stripe",
            "cairn://db?sql=SELECT 1",
            "cairn://posts",
            "file:README.md",
            "file:/etc/hosts",
            "file:../x",
        ] {
            assert!(
                classify(&call, "read_batch", &json!({"paths": [outside]})).is_err(),
                "{outside}"
            );
        }
        // Other read doors are not a way around the per-target check.
        for tool in ["read", "read_web", "read_resource", "read_issue_resource"] {
            assert!(
                classify(&call, tool, &json!({"path": "cairn://p/other/1"})).is_err(),
                "{tool}"
            );
        }
    }

    #[test]
    fn delegated_writes_are_records_never_actors() {
        let call = delegated();
        for (target, mode) in [
            ("cairn://p/cairn/12", "append"),
            ("cairn:@/12/messages", "append"),
            ("cairn://p/cairn/prep/messages", "append"),
        ] {
            assert!(
                classify(
                    &call,
                    "write",
                    &json!({"changes": [{"target": target, "mode": mode}]})
                )
                .is_ok(),
                "{mode} {target}"
            );
        }
        for (target, mode) in [
            ("cairn:@/terminal/x", "create"),
            ("cairn://p/cairn/12/1/builder/terminal/x", "append"),
            ("cairn://p/cairn/12/1/builder/create-pr", "patch"),
            ("cairn://p/cairn/12/1/builder/tasks", "append"),
            ("cairn:@/issues", "append"),
            ("cairn://p/cairn/12", "patch"),
            ("cairn://p/cairn/12", "delete"),
            ("cairn://p/cairn/prep/db/schema", "create"),
            ("cairn://p/cairn/prep/db/attempts", "create"),
            ("cairn://p/cairn/prep/db/attempts/7", "patch"),
            ("cairn://p/cairn/prep/pages/ide", "create"),
        ] {
            assert!(
                classify(
                    &call,
                    "write",
                    &json!({"changes": [{"target": target, "mode": mode}]})
                )
                .is_err(),
                "{mode} {target}"
            );
        }
    }

    #[test]
    fn a_call_never_commits_or_chains() {
        let call = delegated();
        for target in [
            "file:src/lib.rs",
            "cairn:~/tasks",
            "cairn:@/services/echo",
            "cairn://p/cairn/services/judge",
            "cairn://p/other/1",
            "cairn://posts",
        ] {
            assert!(
                classify(&call, "write", &json!({"changes": [{"target": target}]})).is_err(),
                "{target}"
            );
        }
        assert!(classify(
            &call,
            "run",
            &json!({"commands": [{"command": "true"}], "commit_msg": "x"})
        )
        .is_err());
        for key in ["target", "repl", "waitFor"] {
            let mut item = json!({"code": "1"});
            item[key] = json!("x");
            assert!(
                classify(&call, "run", &json!({"commands": [item]})).is_err(),
                "{key}"
            );
        }
        assert!(classify(
            &call,
            "run",
            &json!({"commands": [{"code": "print(1)", "interpreter": "python"}]})
        )
        .is_ok());
        assert!(classify(&call, "check_run", &json!({})).is_err());
    }
}
