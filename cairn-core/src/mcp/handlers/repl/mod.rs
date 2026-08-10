//! Stateful REPL host.
//!
//! A REPL is a persistent interpreter subprocess (an "eval-server") that reads
//! JSON-lines requests on stdin and writes JSON-lines responses on stdout,
//! holding a live namespace across `run` calls.
//!
//! Two halves are deliberately kept apart. The **namespace** is a live interpreter
//! heap owned by the child process and tracked in an in-memory, node-scoped
//! registry on the orchestrator ([`ReplState`], mirroring `pty_state`) — there is
//! no persisting a heap. The **logical REPL** is a durable `job_repls` row with a
//! lifecycle, a language, a dependency set, and a transcript (see [`store`]), so a
//! REPL that dies stays visible with its fate recorded, and creating it again
//! starts the next generation continuing the same transcript.
//!
//! Lifetime = job/worktree lifetime: the always-on orchestrator owns the child, so
//! a REPL survives intra-execution turn suspends (the whole point — state
//! persisting across `run` calls that span turns) and is killed at node/worktree
//! teardown (the hard guarantee against orphans). No interpreter survives a host
//! restart, so the startup reap ([`store::reap_orphans`]) *marks* every stale
//! `running` row exited rather than reconciling it the way terminal recovery does:
//! a recovered REPL process would have an empty namespace and is worth nothing.

pub mod store;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};

use crate::mcp::handlers::RunContext;
use crate::orchestrator::Orchestrator;
use cairn_common::executor_protocol::{
    ProcessSandboxMode, ResidencyFence, ResidencyOperation, ResidencyResult,
    ResidentProcessCwdRoot, ResidentProcessEventKind, ResidentProcessIoMode, ResidentProcessKind,
    ResidentProcessSpec, ResidentProcessStatus, ResidentProcessStream, ResidentRuntimeAsset,
};
use store::{ReplExitReason, ReplRowStatus};

/// Max bytes retained for a single exchange's stdout/stderr capture. Output
/// beyond this is truncated and the exchange is flagged `truncated`.
const OUTPUT_CAP: usize = 64 * 1024;
/// Maximum JSONL frame retained before treating the eval server as desynchronized.
const FRAME_CAP: usize = 1024 * 1024;

/// The embedded python eval-server, materialized to the job scratch dir at REPL
/// creation and run as a script argument (stdin is reserved for the request
/// protocol, so the server cannot also arrive on stdin).
const PYTHON_EVAL_SERVER: &str = include_str!("eval_server.py");

/// The embedded typescript eval-server, run by `bun` with the same agent
/// PATH/env/sandbox as an inline typescript `run` item.
const TYPESCRIPT_EVAL_SERVER: &str = include_str!("eval_server.ts");

/// The interpreter backing a REPL session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplLang {
    Python,
    Typescript,
}

impl ReplLang {
    /// Parse the create-payload `interpreter` string. Mirrors the inline-code
    /// interpreter aliases: `python`/`py` → Python, and `typescript`/`ts`/
    /// `javascript`/`js` → Typescript (bun runs both identically, and a session
    /// created as `typescript` must accept a send tagged `javascript`). Returns
    /// `None` for an unknown interpreter so the caller can name the accepted set.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "python" | "py" => Some(Self::Python),
            "typescript" | "ts" | "javascript" | "js" => Some(Self::Typescript),
            _ => None,
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Python => "python",
            Self::Typescript => "typescript",
        }
    }
}

/// One request's response from the eval-server (the JSON-lines protocol).
#[derive(Debug, Deserialize)]
pub(crate) struct ReplResponse {
    /// `"success"` or `"error"`.
    #[serde(rename = "type")]
    pub kind: String,
    /// `repr` of the final expression's value, omitted when the last statement is
    /// not an expression (or evaluates to `None`).
    #[serde(default)]
    pub value: Option<String>,
    /// Message + traceback, present only on `type:error`.
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    /// Advisory guidance from the eval-server (typescript only; python never sets
    /// it, so serde default keeps it `None`). Currently: a top-level-await send
    /// was auto-wrapped, so its `const`/`let` declarations did not persist.
    #[serde(default)]
    pub note: Option<String>,
    /// Namespace snapshot taken at the end of this evaluation (python only).
    ///
    /// A REPL's namespace can only change as the result of a send, so a snapshot
    /// taken after every eval is not a stale cache — it *is* the live namespace,
    /// always. Piggybacking it on the eval response rather than asking the
    /// interpreter on demand also keeps the read path synchronous and avoids a
    /// real protocol hazard: the response stream is an unkeyed queue, so a
    /// timed-out introspection reply would later be consumed as the next send's
    /// response and desynchronize the session.
    #[serde(default)]
    pub vars: Option<Vec<ReplBinding>>,
}

/// One name bound in a REPL's namespace, as summarized by the eval-server.
///
/// The summary is deliberately cheap and side-effect-free: listing what is bound
/// must never execute user code, so only immutable scalars are `repr`'d and
/// everything else reports its type name (and a length when sized).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplBinding {
    pub name: String,
    /// The value's type name (`DataFrame`, `list`, `module`, ...).
    pub kind: String,
    /// A short scalar repr or `len N`, when one is safely available.
    #[serde(default)]
    pub info: Option<String>,
}

impl ReplResponse {
    pub fn succeeded(&self) -> bool {
        self.kind == "success"
    }
}

/// Who submitted a REPL exchange: the node's own agent (a `run` item's `repl`
/// key) or the user (the REPL tab composer). Both serialize into the one shared
/// namespace; the origin only labels the transcript card.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplOrigin {
    Agent,
    User,
}

impl ReplOrigin {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::User => "user",
        }
    }

    pub(crate) fn parse(raw: &str) -> Option<Self> {
        match raw {
            "agent" => Some(Self::Agent),
            "user" => Some(Self::User),
            _ => None,
        }
    }
}

/// Terminal status of a settled exchange, or `Pending` while in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplExchangeStatus {
    /// Submitted, awaiting the eval-server response.
    Pending,
    /// The eval-server returned `type:success`.
    Success,
    /// The eval-server returned `type:error` (user code raised).
    Error,
    /// No response within the send timeout; the session was killed.
    Timeout,
    /// The child had already exited; state is lost.
    Died,
    /// A framed line that did not parse as the protocol.
    Protocol,
}

impl ReplExchangeStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Success => "success",
            Self::Error => "error",
            Self::Timeout => "timeout",
            Self::Died => "died",
            Self::Protocol => "protocol",
        }
    }

    pub(crate) fn parse(raw: &str) -> Option<Self> {
        Some(match raw {
            "pending" => Self::Pending,
            "success" => Self::Success,
            "error" => Self::Error,
            "timeout" => Self::Timeout,
            "died" => Self::Died,
            "protocol" => Self::Protocol,
            _ => return None,
        })
    }
}

/// One recorded request/response pair in a REPL's durable transcript: a
/// `repl_exchanges` row, inserted pending on submit and updated in place when it
/// settles.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplExchange {
    pub seq: u64,
    /// Which interpreter incarnation ran this send. `seq` is continuous across
    /// generations, so this is what marks where the session restarted.
    pub generation: i64,
    pub origin: ReplOrigin,
    pub code: String,
    /// Epoch milliseconds when the send was submitted.
    pub started_at: i64,
    /// Wall-clock round-trip time, present once settled.
    pub duration_ms: Option<u64>,
    pub status: ReplExchangeStatus,
    pub value: Option<String>,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub error: Option<String>,
    pub note: Option<String>,
    /// True when stdout or stderr was capped at [`OUTPUT_CAP`].
    pub truncated: bool,
}

/// Listing view of one REPL, for facet projection and the tab header. Durable
/// fields come from the `job_repls` row (so an exited REPL still lists, with its
/// fate); `alive`/`busy` come from the live registry. Carries no exchange history
/// (that comes from `get_repl_history`).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplInfo {
    pub job_id: String,
    pub slug: String,
    pub interpreter: String,
    /// Epoch milliseconds when the REPL was first created (not this generation).
    pub created_at: i64,
    /// Which interpreter incarnation is current; a resume bumps it.
    pub generation: i64,
    /// `running` | `exited`.
    pub status: String,
    /// Why the process is gone, when it is.
    pub exit_reason: Option<String>,
    /// The newest exchange's status, which is what colors the facet icon.
    pub last_status: Option<ReplExchangeStatus>,
    pub alive: bool,
    /// A send is currently in flight on this session's `send_lock`.
    pub busy: bool,
}

fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Truncate output to [`OUTPUT_CAP`] bytes (on a char boundary), returning the
/// possibly-shortened string and whether it was cut.
fn cap_output(raw: &str) -> (String, bool) {
    if raw.len() <= OUTPUT_CAP {
        return (raw.to_string(), false);
    }
    let mut end = OUTPUT_CAP;
    while end > 0 && !raw.is_char_boundary(end) {
        end -= 1;
    }
    (raw[..end].to_string(), true)
}

fn some_non_empty(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

/// Outcome of one [`send`] round-trip.
pub(crate) enum ReplSendResult {
    /// The eval-server returned a framed response line.
    Response(ReplResponse),
    /// No response within the item timeout; the caller kills the REPL.
    Timeout,
    /// The child had already exited (or its pipes closed) — state is lost.
    Dead,
    /// A framed line that did not parse as the protocol.
    Protocol(String),
}

/// A live eval-server session held behind an `Arc` in [`ReplState`]. One session
/// is one *generation* of the durable REPL identified by `repl_id`.
pub struct ReplSession {
    pub(crate) interpreter: ReplLang,
    /// The `job_repls` row this generation serves.
    pub repl_id: String,
    /// Project scope stamped onto this session's transcript rows, so they route
    /// with the rest of the project's data. `None` for a project-less test job.
    project_id: Option<String>,
    /// Which incarnation of the REPL this process is. Pairs with the row's own
    /// `generation`: the two must agree, or an exchange records an identity the
    /// row denies.
    pub generation: i64,
    fence: ResidencyFence,
    /// `(repository, logical branch)` this session's checkout must follow.
    /// Carried on the session, resolved once at spawn, so re-aligning before an
    /// eval costs a branch resolution and not a database read.
    ///
    /// Absent for a branchless owner — a thread session, which owns no branch by
    /// construction and runs in the project's live checkout. Nothing there is
    /// Cairn's to move, so such a session has nothing to re-align to.
    managed_branch: Option<(std::path::PathBuf, String)>,
    process_key: String,
    process_generation: u64,
    responses: Mutex<mpsc::Receiver<String>>,
    alive: AtomicBool,
    created_at: SystemTime,
    /// Serializes request->response round-trips so two `run` items targeting the
    /// same slug (items run in parallel by default) cannot interleave on the
    /// single-threaded eval-server. Different slugs stay concurrent.
    send_lock: tokio::sync::Mutex<()>,
    /// Monotonic exchange sequence, shared across agent and user sends and seeded
    /// from the durable transcript so it continues across generations. Allocated
    /// in memory rather than from `MAX(seq)+1` per send because two concurrent
    /// sends to one slug allocate before either takes `send_lock`.
    seq: AtomicU64,
}

impl ReplSession {
    /// Stop the interpreter without touching the lease. The home it runs in
    /// belongs to the job, not to this REPL, and outlives it.
    pub async fn stop_and_release(&self, orch: &Orchestrator) {
        self.alive.store(false, Ordering::Release);
        let _ = crate::fleet::residency::stop(orch, &self.fence, &self.process_key).await;
    }

    /// True while the child is still running (non-blocking).
    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    /// True while a send holds `send_lock` (an exchange is in flight).
    pub fn is_busy(&self) -> bool {
        self.send_lock.try_lock().is_err()
    }

    /// The logical branch this session's checkout follows, or `None` for a
    /// branchless owner living in the project's live checkout.
    pub fn managed_branch(&self) -> Option<&str> {
        self.managed_branch
            .as_ref()
            .map(|(_, branch)| branch.as_str())
    }

    /// Allocate the next exchange sequence number.
    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }
}

/// In-memory, node-scoped registry of live REPL sessions, keyed by
/// `(job_id, slug)`. One instance lives on the orchestrator, shared by every
/// host exactly like `pty_state`.
#[derive(Default)]
pub struct ReplState {
    sessions: Mutex<HashMap<(String, String), Arc<ReplSession>>>,
    /// Per-slug lifecycle serialization. See [`ReplState::lifecycle_lock`].
    lifecycle_locks: LifecycleLocks,
}

/// Per-`(job_id, slug)` lifecycle locks held by [`ReplState`].
type LifecycleLocks = Mutex<HashMap<(String, String), Arc<tokio::sync::Mutex<()>>>>;

impl ReplState {
    /// The lock that serializes *lifecycle transitions* for one slug: opening,
    /// resuming, and closing.
    ///
    /// Creating a generation spans a durable write and a registry claim, and both
    /// must land as one indivisible step. Without this, two concurrent opens each
    /// see a vacant registry, each bumps the row's generation, and only then does
    /// one win the slot — leaving the winning session's generation disagreeing
    /// with the row that names it, and the loser having already mutated the
    /// durable identity it lost.
    ///
    /// Sends deliberately do NOT take this lock: a send can run for minutes, and
    /// serializing lifecycle behind it would stall a close. A send's own exit
    /// recording is fenced by generation instead (see [`store::mark_exited`]).
    pub fn lifecycle_lock(&self, job_id: &str, slug: &str) -> Arc<tokio::sync::Mutex<()>> {
        let Ok(mut locks) = self.lifecycle_locks.lock() else {
            // A poisoned map would serialize nothing; hand back a private lock so
            // the caller still runs rather than deadlocking the surface.
            return Arc::new(tokio::sync::Mutex::new(()));
        };
        locks
            .entry((job_id.to_string(), slug.to_string()))
            .or_default()
            .clone()
    }
    pub fn get(&self, job_id: &str, slug: &str) -> Option<Arc<ReplSession>> {
        self.sessions
            .lock()
            .ok()?
            .get(&(job_id.to_string(), slug.to_string()))
            .cloned()
    }

    pub fn contains(&self, job_id: &str, slug: &str) -> bool {
        self.sessions
            .lock()
            .map(|s| s.contains_key(&(job_id.to_string(), slug.to_string())))
            .unwrap_or(false)
    }

    pub fn insert(&self, job_id: String, slug: String, session: Arc<ReplSession>) {
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.insert((job_id, slug), session);
        }
    }

    pub fn remove(&self, job_id: &str, slug: &str) -> Option<Arc<ReplSession>> {
        self.sessions
            .lock()
            .ok()?
            .remove(&(job_id.to_string(), slug.to_string()))
    }

    /// Remove the session at `(job_id, slug)` only when it is *this exact*
    /// session (`Arc::ptr_eq`). An obsolete operation — a dead/timed-out send that
    /// resolves after the user closed and recreated the same slug — must never
    /// evict the replacement generation. Returns true iff it removed `expected`.
    pub fn remove_if(&self, job_id: &str, slug: &str, expected: &Arc<ReplSession>) -> bool {
        let Ok(mut sessions) = self.sessions.lock() else {
            return false;
        };
        let key = (job_id.to_string(), slug.to_string());
        match sessions.get(&key) {
            Some(current) if Arc::ptr_eq(current, expected) => {
                sessions.remove(&key);
                true
            }
            _ => false,
        }
    }

    /// Insert only when the slot is vacant, so two concurrent creates cannot each
    /// spawn and have the second silently orphan the first's live process.
    /// Returns true iff it inserted `session`.
    pub fn insert_if_absent(
        &self,
        job_id: String,
        slug: String,
        session: Arc<ReplSession>,
    ) -> bool {
        let Ok(mut sessions) = self.sessions.lock() else {
            return false;
        };
        let key = (job_id, slug);
        if sessions.contains_key(&key) {
            return false;
        }
        sessions.insert(key, session);
        true
    }

    /// Drain every session belonging to one of `job_ids` (teardown), returning
    /// each with its `(job_id, slug)` key so the caller can emit a lifecycle
    /// event before killing it.
    pub(crate) fn remove_for_jobs(
        &self,
        job_ids: &[String],
    ) -> Vec<(String, String, Arc<ReplSession>)> {
        let Ok(mut sessions) = self.sessions.lock() else {
            return Vec::new();
        };
        let set: HashSet<&str> = job_ids.iter().map(String::as_str).collect();
        let keys: Vec<(String, String)> = sessions
            .keys()
            .filter(|(job_id, _)| set.contains(job_id.as_str()))
            .cloned()
            .collect();
        let drained: Vec<(String, String, Arc<ReplSession>)> = keys
            .into_iter()
            .filter_map(|(job_id, slug)| {
                sessions
                    .remove(&(job_id.clone(), slug.clone()))
                    .map(|session| (job_id, slug, session))
            })
            .collect();
        // Teardown is the end of the job, so no further lifecycle transition can
        // arrive for these slugs; drop their locks rather than retaining one per
        // slug for the life of the process.
        if let Ok(mut locks) = self.lifecycle_locks.lock() {
            locks.retain(|(job_id, _), _| !set.contains(job_id.as_str()));
        }
        drained
    }

    /// Liveness of every registered session, keyed `(job_id, slug)`, for joining
    /// onto a durable listing in one pass.
    pub fn liveness(&self) -> HashMap<(String, String), (bool, bool)> {
        let Ok(sessions) = self.sessions.lock() else {
            return HashMap::new();
        };
        sessions
            .iter()
            .map(|(key, session)| (key.clone(), (session.is_alive(), session.is_busy())))
            .collect()
    }
}

/// The sandbox a REPL's interpreter spawns under, in the two values the resident
/// spec carries: the mode it declares, and the policy computed host-side when one
/// applies.
///
/// Two things decide it, and they are separate questions. **Which checkout** the
/// interpreter runs in follows the owner's branch: a REPL on a branch runs in the
/// job's execution home, the agent's own writable checkout however little it looks
/// like one on disk, while a branchless one runs in the project's live checkout,
/// somebody else's working tree. The policy is built against that checkout rather
/// than against the controller's scratch directory, so the read-only shape can
/// drop any session grant that would otherwise reopen the live tree.
///
/// **Whether anything is enforced** follows the owner's fence, exactly as a
/// terminal's does — and the fence belongs to the REPL's *owner*, not to whoever
/// asked for it. A UI create carries no run context at all, so deriving the
/// identity from the caller answered "nobody's agent operation", built no policy,
/// and spawned the interpreter unconfined. Resolving the owner's own run here is
/// what a terminal already does from its job row (`resolve_terminal_resource_target`
/// reads the latest run alongside the branch), so a REPL and the terminal beside
/// it in one cell are confined alike however each was opened.
pub async fn repl_sandbox(
    orch: &Orchestrator,
    job_id: &str,
    project_id: &str,
    scratch_cwd: &str,
    repo_path: &str,
    managed_branch: Option<&str>,
    run_context: Option<&RunContext>,
) -> (
    ProcessSandboxMode,
    Option<crate::services::sandbox::SandboxPolicy>,
) {
    let (checkout_kind, policy_checkout) = match managed_branch {
        Some(_) => (
            crate::mcp::handlers::run::RunCheckout::AgentOwned,
            scratch_cwd,
        ),
        None => (
            crate::mcp::handlers::run::RunCheckout::ProjectLive,
            repo_path,
        ),
    };
    let fence_run_id = match run_context {
        Some(ctx) => Some(ctx.run_id.clone()),
        None => crate::jobs::queries::latest_run_id_for_job(&orch.db.local, job_id).await,
    };
    let policy = crate::mcp::handlers::run::build_run_sandbox_policy(
        orch,
        policy_checkout,
        checkout_kind,
        fence_run_id.as_deref(),
        Some(project_id),
        None,
    )
    .await
    .map(|(policy, _)| policy);
    // No policy means the owner's fence resolved to `allow`, or resolved to
    // nothing at all: an unfenced spawn, declared as one rather than dressed up
    // in a mode the executor would then have to invent a policy for.
    let mode = match (policy.is_some(), checkout_kind) {
        (false, _) => ProcessSandboxMode::Unconfined,
        (true, crate::mcp::handlers::run::RunCheckout::AgentOwned) => ProcessSandboxMode::Confined,
        (true, crate::mcp::handlers::run::RunCheckout::ProjectLive) => {
            ProcessSandboxMode::ReadOnlyCheckout
        }
    };
    (mode, policy)
}

/// Start an eval server in an executor residency.
#[allow(clippy::too_many_arguments)]
pub async fn spawn_session(
    orch: &Orchestrator,
    job_id: &str,
    project_id: &str,
    cwd: &str,
    run_context: Option<&RunContext>,
    interpreter: ReplLang,
    slug: &str,
    deps: &[String],
) -> Result<Arc<ReplSession>, String> {
    use crate::storage::RowExt;
    let job = job_id.to_string();
    let (branch, base_branch, repo_path) = orch.db.local.read(|conn| Box::pin(async move {
        let mut rows = conn.query("SELECT j.branch, j.base_branch, p.repo_path FROM jobs j JOIN projects p ON p.id=j.project_id WHERE j.id=?1", [job.as_str()]).await?;
        let row = rows.next().await?.ok_or_else(|| crate::storage::DbError::Row(format!("Job not found: {job}")))?;
        Ok((row.opt_text(0)?, row.opt_text(1)?, row.text(2)?))
    })).await.map_err(|e| e.to_string())?;
    // A job with neither is branchless by construction rather than incomplete: a
    // thread session is commit-fenced, owns no branch and no PR, and there is no
    // managed checkout to resolve for it. Its REPL lives in the project's live
    // checkout instead, exactly where its terminals already run, so the two share
    // one environment.
    let managed_branch = branch.or(base_branch);

    let (asset, body) = match interpreter {
        ReplLang::Python => ("repl/eval_server.py", PYTHON_EVAL_SERVER),
        ReplLang::Typescript => ("repl/eval_server.ts", TYPESCRIPT_EVAL_SERVER),
    };
    let (program, args): (String, Vec<String>) = match interpreter {
        ReplLang::Python if !deps.is_empty() => {
            let mut args = vec!["run".into()];
            for dep in deps {
                args.extend(["--with".into(), dep.clone()]);
            }
            args.extend([
                "python3".into(),
                "-c".into(),
                "import os; exec(compile(open(os.path.join(os.environ['CAIRN_RUNTIME_ASSETS'], 'repl/eval_server.py'), encoding='utf-8').read(), 'eval_server.py', 'exec'))".into(),
            ]);
            ("uv".into(), args)
        }
        ReplLang::Python => {
            (
                "python3".into(),
                vec![
                    "-c".into(),
                    "import os; exec(compile(open(os.path.join(os.environ['CAIRN_RUNTIME_ASSETS'], 'repl/eval_server.py'), encoding='utf-8').read(), 'eval_server.py', 'exec'))".into(),
                ],
            )
        }
        ReplLang::Typescript => {
            if !deps.is_empty() {
                return Err("REPL deps are python-only".into());
            }
            (
                "bun".into(),
                vec![
                    "-e".into(),
                    "await import(process.env.CAIRN_RUNTIME_ASSETS + '/repl/eval_server.ts')"
                        .into(),
                ],
            )
        }
    };
    let (sandbox_mode, policy) = repl_sandbox(
        orch,
        job_id,
        project_id,
        cwd,
        &repo_path,
        managed_branch.as_deref(),
        run_context,
    )
    .await;
    let config = crate::mcp::handlers::run::build_agent_spawn_config(
        orch,
        cwd,
        run_context,
        &program,
        &args,
        policy,
    )
    .await;
    let sandbox_policy =
        config
            .sandbox
            .as_ref()
            .map(|p| cairn_common::executor_protocol::ResidentSandboxPolicy {
                worktree: p.worktree.to_string_lossy().into_owned(),
                writable_extra: p
                    .writable_extra
                    .iter()
                    .map(|x| x.to_string_lossy().into_owned())
                    .collect(),
                deny_read: p
                    .deny_read
                    .iter()
                    .map(|x| x.to_string_lossy().into_owned())
                    .collect(),
                writable_regex: p.writable_regex.clone(),
                worktree_writable: p.worktree_writable,
            });
    let env = config
        .env
        .into_iter()
        .filter(|(k, _)| !matches!(k.as_str(), "CAIRN_WORKTREE" | "TMPDIR" | "TMP" | "TEMP"))
        .collect();
    // A REPL is another long-lived process in its owner's one execution home, so
    // its interpreter sees the same installed packages, the same `$TMPDIR`, and
    // the same absolute paths as that owner's run batches and terminals.
    let fleet_config = crate::config::settings::load_fleet(&orch.config_dir);
    let wait_horizon_unix_ms = crate::fleet::default_wait_horizon_unix_ms(&fleet_config);
    let fence = match managed_branch.as_deref() {
        Some(branch) => {
            let tip = crate::fleet::residency::resolve_logical_commit(
                orch,
                std::path::Path::new(&repo_path),
                branch,
            )
            .await?;
            let fence = crate::fleet::residency::acquire_job_residency(
                orch,
                &orch.db.local,
                job_id,
                &tip,
                wait_horizon_unix_ms,
                crate::fleet::unix_time_ms(),
            )
            .await
            .map_err(|refusal| refusal.diagnostic)?;
            crate::fleet::residency::refresh(orch, &fence, &tip).await?;
            fence
        }
        // The live checkout is externally owned and always at its own HEAD, so
        // there is no checkout to force to a commit here and no refresh to send:
        // a refresh would be Cairn moving somebody else's working tree.
        None => {
            let owner_ref = crate::fleet::residency::job_residence(&orch.db.local, job_id)
                .await
                .ok()
                .map(|residence| residence.owner_ref);
            let request = crate::fleet::residency::live_checkout_residency_request(
                job_id,
                project_id,
                &repo_path,
                owner_ref,
                wait_horizon_unix_ms,
            )
            .await?;
            let fence = crate::fleet::residency::acquire(orch, request)
                .await
                .map_err(|refusal| refusal.diagnostic)?;
            let _ = crate::fleet::residency::renew(orch, &fence).await;
            fence
        }
    };

    // Process keys are unique within the home, so a REPL names itself by slug
    // rather than claiming the one generic key.
    let key = format!("repl:{slug}");
    let (tx, rx) = mpsc::sync_channel(2);
    let buffer = Arc::new(Mutex::new(Vec::<u8>::new()));
    let early_alive = Arc::new(AtomicBool::new(true));
    let ef = fence.clone();
    let ek = key.clone();
    let eb = buffer.clone();
    let ea = early_alive.clone();
    orch.fleet.subscribe_resident_process_events(move |event| {
        if event.holder != ef.holder
            || event.incarnation_id != ef.incarnation_id
            || event.cell_epoch != ef.cell_epoch
            || event.process_key != ek
        {
            return;
        }
        match event.event {
            ResidentProcessEventKind::Output {
                stream: ResidentProcessStream::Stdout,
                data,
                ..
            } => {
                if let Ok(mut b) = eb.lock() {
                    if b.len().saturating_add(data.len()) > FRAME_CAP {
                        b.clear();
                        ea.store(false, Ordering::Release);
                        let _ = tx.try_send("oversized REPL protocol frame".to_string());
                        return;
                    }
                    b.extend(data);
                    while let Some(i) = b.iter().position(|x| *x == b'\n') {
                        let line = String::from_utf8_lossy(&b[..i]).into_owned();
                        b.drain(..=i);
                        if tx.try_send(line).is_err() {
                            ea.store(false, Ordering::Release);
                            break;
                        }
                    }
                }
            }
            ResidentProcessEventKind::Output {
                stream: ResidentProcessStream::Stderr,
                data,
                ..
            } => tracing::debug!(diagnostic=%String::from_utf8_lossy(&data),"REPL stderr"),
            ResidentProcessEventKind::State {
                status: ResidentProcessStatus::Exited { .. },
            } => ea.store(false, Ordering::Release),
            _ => {}
        }
    });
    let started = orch
        .fleet
        .operate_residency(
            orch,
            ResidencyOperation::StartProcess {
                fence: fence.clone(),
                process_key: key.clone(),
                kind: ResidentProcessKind::Repl {
                    slug: slug.to_string(),
                },
                // A REPL session declares nothing: an idle interpreter and one
                // grinding through a dataframe are the same live process, and
                // only measurement can tell them apart.
                reservation: None,
                process: ResidentProcessSpec {
                    program,
                    args,
                    cwd: String::new(),
                    cwd_root: ResidentProcessCwdRoot::Checkout,
                    env,
                    sandbox_mode,
                    sandbox_policy,
                    runtime_assets: vec![ResidentRuntimeAsset {
                        path: asset.into(),
                        data: body.as_bytes().to_vec(),
                    }],
                    io: ResidentProcessIoMode::Pipe,
                },
            },
        )
        .await;
    let ResidencyResult::State { cell } = started else {
        let _ = crate::fleet::residency::release(orch, &fence).await;
        return Err(format!("failed to start REPL: {started:?}"));
    };
    let Some(generation) = cell.occupancy.processes.get(&key).map(|p| p.generation) else {
        let _ = crate::fleet::residency::stop(orch, &fence, &key).await;
        return Err("REPL start returned no process generation".to_string());
    };
    // The process is up, so open a generation on the durable row: insert it on a
    // first create, or bump an exited row back to running on a resume. Doing this
    // after the spawn means a failed spawn never leaves a row claiming to run.
    let project_ref = (!project_id.is_empty()).then_some(project_id);
    let record =
        match store::begin(&orch.db.local, job_id, project_ref, slug, interpreter, deps).await {
            Ok(record) => record,
            Err(error) => {
                let _ = crate::fleet::residency::stop(orch, &fence, &key).await;
                return Err(format!("failed to record REPL '{slug}': {error}"));
            }
        };
    let start_seq = store::next_seq(&orch.db.local, &record.id)
        .await
        .unwrap_or(0);
    let session = Arc::new(ReplSession {
        interpreter,
        repl_id: record.id.clone(),
        project_id: record.project_id.clone(),
        generation: record.generation,
        fence: fence.clone(),
        managed_branch: managed_branch
            .clone()
            .map(|branch| (std::path::PathBuf::from(&repo_path), branch)),
        process_key: key,
        process_generation: generation,
        responses: Mutex::new(rx),
        alive: AtomicBool::new(early_alive.load(Ordering::Acquire)),
        created_at: SystemTime::now(),
        send_lock: tokio::sync::Mutex::new(()),
        seq: AtomicU64::new(start_seq),
    });
    let weak = Arc::downgrade(&session);
    let xf = fence.clone();
    let xk = session.process_key.clone();
    orch.fleet.subscribe_resident_process_events(move |e| {
        if e.holder == xf.holder
            && e.incarnation_id == xf.incarnation_id
            && e.cell_epoch == xf.cell_epoch
            && e.process_key == xk
            && e.process_generation == generation
            && matches!(
                e.event,
                ResidentProcessEventKind::State {
                    status: ResidentProcessStatus::Exited { .. }
                }
            )
        {
            if let Some(s) = weak.upgrade() {
                s.alive.store(false, Ordering::Release);
            }
        }
    });
    let ro = orch.clone();
    let rf = fence;
    let rs = Arc::downgrade(&session);
    tokio::spawn(async move {
        let mut i = tokio::time::interval(Duration::from_secs(60));
        i.tick().await;
        loop {
            i.tick().await;
            let Some(s) = rs.upgrade() else { break };
            if !s.is_alive() {
                break;
            }
            if crate::fleet::residency::renew(&ro, &rf).await.is_err() {
                s.alive.store(false, Ordering::Release);
                break;
            }
        }
    });
    Ok(session)
}

pub(crate) async fn send(
    orch: &Orchestrator,
    session: &Arc<ReplSession>,
    code: &str,
    timeout: Duration,
) -> ReplSendResult {
    let _guard = session.send_lock.lock().await;
    if !session.is_alive() {
        return ReplSendResult::Dead;
    }
    let mut data = serde_json::json!({"code":code}).to_string().into_bytes();
    data.push(b'\n');
    let result = orch
        .fleet
        .operate_residency(
            orch,
            ResidencyOperation::WriteProcessInput {
                fence: session.fence.clone(),
                process_key: session.process_key.clone(),
                process_generation: session.process_generation,
                data,
            },
        )
        .await;
    if !matches!(result, ResidencyResult::State { .. }) {
        return ReplSendResult::Dead;
    }
    let s = session.clone();
    let recv = tokio::task::spawn_blocking(move || {
        s.responses
            .lock()
            .map_err(|_| ())?
            .recv_timeout(timeout)
            .map_err(|_| ())
    })
    .await;
    match recv {
        Ok(Ok(line)) => serde_json::from_str::<ReplResponse>(&line)
            .map(ReplSendResult::Response)
            .unwrap_or_else(|e| {
                ReplSendResult::Protocol(format!("unparseable eval-server response: {e}: {line}"))
            }),
        _ if session.is_alive() => ReplSendResult::Timeout,
        _ => ReplSendResult::Dead,
    }
}

/// Announce a transcript change. A REPL exchange is two discrete state changes
/// (submitted, settled), not a byte stream, so the standard `db-change` emit —
/// the one path every other entity in the app already invalidates through — is
/// exactly right, and it works app-wide instead of dying with the REPL pane.
pub(crate) fn emit_exchange_change(orch: &Orchestrator) {
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "repl_exchanges", "action": "update"}),
    );
}

/// Announce a REPL lifecycle change (created, resumed, exited, removed).
pub(crate) fn emit_repl_change(orch: &Orchestrator, action: &str) {
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_repls", "action": action}),
    );
}

/// The one hint an agent sees when a slug has no REPL at all. Names `deps`,
/// because the capability existed from the start yet every surface's example
/// omitted it — which is why users fell back to shelling out to `pip install`.
pub(crate) fn create_hint(slug: &str) -> String {
    format!(
        "Create it: write cairn:~/repl/{slug} {{interpreter:\"python\", deps:[\"pandas\"]}} \
         (interpreter python | typescript; deps preloads python packages via uv)"
    )
}

/// The one canonical send funnel: record a pending exchange row, run the send,
/// perform any Dead/Timeout kill-and-unregister, settle that same row in place,
/// and announce both writes as `db-change` (plus a `job_repls` change on a
/// session-ending outcome). Both the agent path (`run` item) and the user path
/// (REPL tab composer) route through here so every exchange is recorded and
/// broadcast identically.
///
/// Fails closed (`Err`) on a precondition that predates any exchange — an
/// unknown slug or a language mismatch — so the caller can surface the hint
/// without a phantom transcript card. Once an exchange exists, the outcome is
/// always `Ok(exchange)`; the exchange's `status` carries success/error/died/
/// timeout/protocol.
pub async fn send_recorded(
    orch: &Orchestrator,
    job_id: &str,
    slug: &str,
    code: &str,
    timeout: Duration,
    origin: ReplOrigin,
    expected_lang: Option<ReplLang>,
) -> Result<ReplExchange, String> {
    let session = orch.repl_state.get(job_id, slug).ok_or_else(|| {
        format!(
            "No REPL named '{slug}' is running for this node. {}",
            create_hint(slug)
        )
    })?;
    if let Some(lang) = expected_lang {
        if session.interpreter != lang {
            return Err(format!(
                "REPL '{slug}' is a {} session; this send used interpreter '{}'. Match the REPL's language.",
                session.interpreter.label(),
                lang.label()
            ));
        }
    }

    // Every eval is a newly delivered unit of execution, so it runs against the
    // logical head rather than against whatever the interpreter's checkout held
    // when it spawned. A REPL is another long-lived process in the job's one
    // execution home, and aligning only at spawn left it compiling and importing
    // pre-write source for the rest of its life.
    //
    // Fails closed here, before any exchange row exists, for the same reason the
    // slug and language checks do: a precondition that predates the exchange
    // belongs in the caller's error, not in a phantom transcript card.
    //
    // A branchless session has nothing to align: it lives in the project's live
    // checkout, which is somebody else's working tree at whatever commit they
    // have it on, and moving it would be Cairn editing their tree.
    if let Some((repo_path, branch)) = session.managed_branch.as_ref() {
        let tip = crate::fleet::residency::resolve_logical_commit(orch, repo_path, branch).await?;
        crate::fleet::residency::refresh(orch, &session.fence, &tip)
            .await
            .map_err(|error| {
                format!(
                    "REPL '{slug}' was not sent code because its checkout could not be aligned to \
                     the logical head {tip}: {error}"
                )
            })?;
    }

    let seq = session.next_seq();
    let mut exchange = ReplExchange {
        seq,
        generation: session.generation,
        origin,
        code: code.to_string(),
        started_at: now_millis(),
        duration_ms: None,
        status: ReplExchangeStatus::Pending,
        value: None,
        stdout: None,
        stderr: None,
        error: None,
        note: None,
        truncated: false,
    };
    if let Err(error) = store::insert_pending(
        &orch.db.local,
        &session.repl_id,
        session.project_id.as_deref(),
        &exchange,
    )
    .await
    {
        tracing::warn!(%error, slug, "failed to record pending REPL exchange");
    }
    emit_exchange_change(orch);

    let started = Instant::now();
    let result = send(orch, &session, code, timeout).await;
    exchange.duration_ms = Some(started.elapsed().as_millis() as u64);

    let mut session_ended = false;
    let mut exit_reason = None;
    let mut bindings = None;
    match result {
        ReplSendResult::Response(response) => {
            exchange.status = if response.succeeded() {
                ReplExchangeStatus::Success
            } else {
                ReplExchangeStatus::Error
            };
            exchange.value = response.value.and_then(some_non_empty);
            let (stdout, cut_out) = cap_output(response.stdout.trim_end_matches('\n'));
            exchange.stdout = some_non_empty(stdout);
            let (stderr, cut_err) = cap_output(response.stderr.trim_end_matches('\n'));
            exchange.stderr = some_non_empty(stderr);
            exchange.error = response
                .error
                .map(|e| e.trim_end_matches('\n').to_string())
                .and_then(some_non_empty);
            exchange.note = response
                .note
                .map(|n| n.trim().to_string())
                .and_then(some_non_empty);
            exchange.truncated = cut_out || cut_err;
            bindings = response.vars;
        }
        ReplSendResult::Dead => {
            // Unregister only if this is still the live session: a close+recreate
            // during the send may have installed a new generation under this slug,
            // which this obsolete outcome must not evict.
            session_ended = orch.repl_state.remove_if(job_id, slug, &session);
            session.stop_and_release(orch).await;
            exit_reason = Some(ReplExitReason::Died);
            exchange.status = ReplExchangeStatus::Died;
            exchange.error = Some(format!(
                "REPL '{slug}' died — its namespace is gone. The transcript is kept; \
                 create the slug again to resume it with an empty namespace."
            ));
        }
        ReplSendResult::Timeout => {
            // Kill the timed-out child we hold, but unregister only when it is
            // still the registered session, so a replacement generation created
            // during the send is never removed or killed by this stale outcome.
            session_ended = orch.repl_state.remove_if(job_id, slug, &session);
            session.stop_and_release(orch).await;
            exit_reason = Some(ReplExitReason::Timeout);
            exchange.status = ReplExchangeStatus::Timeout;
            exchange.error = Some(format!(
                "REPL '{slug}' send timed out after {}ms; the interpreter was killed and its \
                 namespace lost. The transcript is kept — create the slug again to resume it, \
                 and break long-running work into smaller sends.",
                timeout.as_millis()
            ));
        }
        ReplSendResult::Protocol(message) => {
            session_ended = orch.repl_state.remove_if(job_id, slug, &session);
            session.stop_and_release(orch).await;
            exit_reason = Some(ReplExitReason::Protocol);
            exchange.status = ReplExchangeStatus::Protocol;
            exchange.error = Some(format!(
                "{message}; the namespace is lost. The transcript is kept — create the slug \
                 again to resume it."
            ));
        }
    }

    if let Err(error) = store::settle(&orch.db.local, &session.repl_id, &exchange).await {
        tracing::warn!(%error, slug, "failed to settle REPL exchange");
    }
    // The namespace can only have changed as a result of this send, so the
    // snapshot that rode back on the response IS the live namespace.
    if let Some(vars) = bindings.as_deref() {
        // Generation-fenced: this snapshot describes THIS generation's namespace,
        // and a resume during the settle would otherwise inherit it and report
        // bindings its empty interpreter does not have.
        if let Err(error) =
            store::set_bindings(&orch.db.local, &session.repl_id, session.generation, vars).await
        {
            tracing::warn!(%error, slug, "failed to record REPL bindings");
        }
    }
    emit_exchange_change(orch);
    // Only when this session is still the registered one: a close+recreate during
    // the send installs a new generation on the same row, which this obsolete
    // outcome must not declare dead.
    if session_ended {
        if let Some(reason) = exit_reason {
            // Generation-fenced: `session_ended` proves this session owned the
            // registry slot at removal time, but `stop_and_release` above yields,
            // and a resume can install the next generation in that window. The
            // guard makes this update match zero rows in that case rather than
            // declaring the live replacement dead.
            if let Err(error) =
                store::mark_exited(&orch.db.local, &session.repl_id, session.generation, reason)
                    .await
            {
                tracing::warn!(%error, slug, "failed to mark REPL exited");
            }
        }
        emit_repl_change(orch, "update");
    }
    Ok(exchange)
}

/// Render a REPL read: a status banner plus the live namespace.
///
/// The banner is driven by the durable row, so an exited REPL reads as *exited*
/// with its fate and its transcript intact rather than as `not found` — death is
/// the most significant event in a REPL's life and must be legible. The registry
/// contributes only liveness and uptime for the current generation. Stays
/// synchronous and infallible: everything it renders was already loaded.
pub fn render_status(
    slug: &str,
    record: Option<&store::ReplRecord>,
    session: Option<&Arc<ReplSession>>,
) -> String {
    let Some(record) = record else {
        return format!(
            "[repl {slug}: not found] No REPL named '{slug}' for this node. {}",
            create_hint(slug)
        );
    };
    let live = session.filter(|s| s.is_alive());
    let running = live.is_some() && record.status == ReplRowStatus::Running;

    let mut banner = format!("[repl {slug}: {}", record.interpreter.label());
    if running {
        let uptime = live
            .and_then(|s| s.created_at.elapsed().ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);
        banner.push_str(&format!(", running, up {uptime}s"));
    } else {
        let reason = record.exit_reason.unwrap_or(ReplExitReason::Closed);
        banner.push_str(&format!(", exited ({})", reason.as_str()));
    }
    banner.push_str(&format!(", gen {}", record.generation));
    banner.push_str(&format!(
        ", {} exchange{}",
        record.exchange_count,
        if record.exchange_count == 1 { "" } else { "s" }
    ));
    if record.bindings.is_empty() {
        banner.push_str(", no bindings");
    } else {
        banner.push_str(&format!(", {} bindings", record.bindings.len()));
    }
    banner.push(']');

    let mut out = banner;
    if !record.bindings.is_empty() {
        let name_width = record
            .bindings
            .iter()
            .map(|b| b.name.len())
            .max()
            .unwrap_or(0);
        let kind_width = record
            .bindings
            .iter()
            .map(|b| b.kind.len())
            .max()
            .unwrap_or(0);
        for binding in &record.bindings {
            out.push('\n');
            match binding.info.as_deref() {
                Some(info) => out.push_str(&format!(
                    "{:name_width$}  {:kind_width$}  {info}",
                    binding.name, binding.kind
                )),
                None => out.push_str(&format!("{:name_width$}  {}", binding.name, binding.kind)),
            }
        }
    }
    if record.interpreter == ReplLang::Typescript {
        out.push_str(
            "\n(binding introspection is python-only: a TS REPL's top-level declarations live \
             in the global lexical environment, which has no enumeration API)",
        );
    }
    if !running {
        let reason = record.exit_reason.unwrap_or(ReplExitReason::Closed);
        out.push_str(&format!(
            "\nThis REPL is not running ({}). Its transcript is kept. Resume it with \
             write cairn:~/repl/{slug} {{mode:\"create\"}} — the interpreter and deps are \
             inherited and the transcript continues, but THE NAMESPACE STARTS EMPTY. \
             Discard it instead with mode:\"delete\".",
            reason.describe()
        ));
    }
    out
}

/// Resolve the job id a `NodeRepl` URI's coordinates address.
///
/// Owner-aware through the shared resolver, so a thread session's REPL resolves
/// to its session job. This was a sixth issue-shaped copy of the coordinate
/// query: `repl/<slug>` from a thread normalized, reached dispatch, and then
/// failed here with a `0/0` placeholder in the error — exactly the leak the
/// reserved coordinate exists to prevent.
pub(crate) async fn resolve_node_repl_job_id(
    db: &crate::storage::LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
) -> Option<String> {
    crate::jobs::queries::job_id_for_node_coordinate(
        db,
        project_key,
        number,
        exec_seq,
        node_id,
        None,
    )
    .await
    .ok()
    .flatten()
}

#[cfg(test)]
mod cap_output_tests {
    use super::{cap_output, OUTPUT_CAP};

    #[test]
    fn short_and_exact_output_is_not_truncated() {
        let (out, cut) = cap_output("hello");
        assert_eq!(out, "hello");
        assert!(!cut);

        let exact = "a".repeat(OUTPUT_CAP);
        let (out, cut) = cap_output(&exact);
        assert_eq!(out.len(), OUTPUT_CAP);
        assert!(!cut);
    }

    #[test]
    fn oversized_output_is_capped_and_flagged() {
        let big = "b".repeat(OUTPUT_CAP + 100);
        let (out, cut) = cap_output(&big);
        assert!(cut);
        assert!(out.len() <= OUTPUT_CAP);
    }

    #[test]
    fn truncation_lands_on_a_char_boundary() {
        // A 3-byte char straddling the cap must be dropped whole, not split.
        let mut s = "x".repeat(OUTPUT_CAP - 1);
        s.push('\u{20AC}'); // euro sign occupies OUTPUT_CAP-1..OUTPUT_CAP+1
        let (out, cut) = cap_output(&s);
        assert!(cut);
        assert!(out.is_char_boundary(out.len()));
        assert_eq!(out.len(), OUTPUT_CAP - 1);
    }
}

#[cfg(test)]
mod lang_tests {
    use super::ReplLang;

    #[test]
    fn parse_aliases_map_to_the_two_languages() {
        for raw in ["python", "py", "PYTHON", " Py "] {
            assert_eq!(ReplLang::parse(raw), Some(ReplLang::Python), "{raw}");
        }
        // typescript, javascript, and their short aliases all resolve to one
        // Typescript session kind (bun runs them identically).
        for raw in ["typescript", "ts", "javascript", "js", "TypeScript", " JS "] {
            assert_eq!(ReplLang::parse(raw), Some(ReplLang::Typescript), "{raw}");
        }
        assert_eq!(ReplLang::parse("ruby"), None);
        assert_eq!(ReplLang::parse(""), None);
    }

    #[test]
    fn label_round_trips_the_canonical_name() {
        assert_eq!(ReplLang::Python.label(), "python");
        assert_eq!(ReplLang::Typescript.label(), "typescript");
    }
}

#[cfg(test)]
mod render_status_tests {
    use super::store::{ReplExitReason, ReplRecord, ReplRowStatus};
    use super::*;

    fn record(interpreter: ReplLang, status: ReplRowStatus) -> ReplRecord {
        ReplRecord {
            id: "repl-1".into(),
            job_id: "job-1".into(),
            project_id: None,
            slug: "lex".into(),
            interpreter,
            deps: Vec::new(),
            generation: 2,
            status,
            exit_reason: (status == ReplRowStatus::Exited).then_some(ReplExitReason::HostRestart),
            bindings: Vec::new(),
            created_at: 0,
            exited_at: None,
            exchange_count: 3,
        }
    }

    #[test]
    fn an_unknown_slug_names_deps_in_its_create_hint() {
        // `deps` has existed since the first REPL commit, but every surface's
        // example omitted it, so users fell back to `pip install` inside the
        // session. The hint an agent actually reads must show it.
        let out = render_status("lex", None, None);
        assert!(out.contains("not found"), "got: {out}");
        assert!(out.contains("deps:[\"pandas\"]"), "got: {out}");
    }

    #[test]
    fn an_exited_repl_renders_its_fate_and_how_to_resume() {
        let out = render_status(
            "lex",
            Some(&record(ReplLang::Python, ReplRowStatus::Exited)),
            None,
        );
        assert!(!out.contains("not found"), "got: {out}");
        assert!(out.contains("exited (host_restart)"), "got: {out}");
        assert!(out.contains("gen 2"), "got: {out}");
        assert!(out.contains("3 exchanges"), "got: {out}");
        // Resuming is only honest if it says the namespace does not come back.
        assert!(out.contains("NAMESPACE STARTS EMPTY"), "got: {out}");
    }

    #[test]
    fn bindings_render_as_a_table_under_the_banner() {
        let mut rec = record(ReplLang::Python, ReplRowStatus::Exited);
        rec.bindings = vec![
            ReplBinding {
                name: "df".into(),
                kind: "DataFrame".into(),
                info: Some("len 1000".into()),
            },
            ReplBinding {
                name: "pd".into(),
                kind: "module".into(),
                info: None,
            },
        ];
        let out = render_status("lex", Some(&rec), None);
        assert!(out.contains("2 bindings"), "got: {out}");
        assert!(out.contains("df  DataFrame  len 1000"), "got: {out}");
        assert!(out.contains("pd  module"), "got: {out}");
    }

    #[test]
    fn a_typescript_read_explains_why_bindings_are_python_only() {
        let out = render_status(
            "ts",
            Some(&record(ReplLang::Typescript, ReplRowStatus::Exited)),
            None,
        );
        assert!(out.contains("python-only"), "got: {out}");
        assert!(out.contains("global lexical environment"), "got: {out}");
    }
}

#[cfg(test)]
mod eval_server_tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Child, Command, Stdio};

    /// A live python eval-server subprocess for direct JSON-in/JSON-out tests.
    struct EvalServer {
        child: Child,
        stdin: std::process::ChildStdin,
        stdout: BufReader<std::process::ChildStdout>,
        _dir: tempfile::TempDir,
    }

    impl EvalServer {
        /// Spawn `<program> <materialized script>`, or `None` if the interpreter
        /// is not available in the test environment (so the caller can skip).
        fn start_with(program: &str, filename: &str, body: &str) -> Option<Self> {
            let dir = tempfile::tempdir().ok()?;
            let script = dir.path().join(filename);
            std::fs::write(&script, body).ok()?;
            let mut child = Command::new(program)
                .arg(&script)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .ok()?;
            let stdin = child.stdin.take()?;
            let stdout = BufReader::new(child.stdout.take()?);
            Some(Self {
                child,
                stdin,
                stdout,
                _dir: dir,
            })
        }

        /// Spawn `python3 <materialized eval_server.py>`, or `None` if python3 is
        /// not available in the test environment.
        fn start() -> Option<Self> {
            Self::start_with("python3", "eval_server.py", PYTHON_EVAL_SERVER)
        }

        /// Spawn `bun <materialized eval_server.ts>`, or `None` if bun is not
        /// available in the test environment.
        fn start_ts() -> Option<Self> {
            Self::start_with("bun", "eval_server.ts", TYPESCRIPT_EVAL_SERVER)
        }

        fn eval(&mut self, code: &str) -> ReplResponse {
            self.send_raw(&serde_json::json!({ "code": code }).to_string())
        }

        /// Write one raw framed line and read one framed response — used to drive
        /// a deliberately malformed request past the `eval` JSON wrapper.
        fn send_raw(&mut self, raw: &str) -> ReplResponse {
            self.stdin.write_all(raw.as_bytes()).unwrap();
            self.stdin.write_all(b"\n").unwrap();
            self.stdin.flush().unwrap();
            let mut line = String::new();
            self.stdout.read_line(&mut line).unwrap();
            serde_json::from_str(&line)
                .unwrap_or_else(|e| panic!("unparseable response {line:?}: {e}"))
        }
    }

    impl Drop for EvalServer {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    #[test]
    fn state_persists_across_requests() {
        let Some(mut server) = EvalServer::start() else {
            eprintln!("skipping: python3 not available");
            return;
        };
        assert!(server.eval("x = 41").succeeded());
        let r = server.eval("x + 1");
        assert!(r.succeeded());
        assert_eq!(r.value.as_deref(), Some("42"));
    }

    #[test]
    fn defs_persist_and_are_callable_later() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        assert!(server.eval("def double(n):\n    return n * 2").succeeded());
        let r = server.eval("double(21)");
        assert_eq!(r.value.as_deref(), Some("42"));
    }

    #[test]
    fn trailing_expression_yields_value_statement_does_not() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        let expr = server.eval("1 + 2");
        assert_eq!(expr.value.as_deref(), Some("3"));
        let stmt = server.eval("y = 5");
        assert_eq!(stmt.value, None);
    }

    #[test]
    fn print_captured_without_breaking_framing() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        let r = server.eval("print('hello')\nprint('world')");
        assert!(r.succeeded());
        assert_eq!(r.stdout, "hello\nworld\n");
        // A later request still frames correctly (stdout was restored).
        assert_eq!(server.eval("7 * 6").value.as_deref(), Some("42"));
    }

    #[test]
    fn exception_returns_error_with_traceback() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        let r = server.eval("1 / 0");
        assert!(!r.succeeded());
        assert_eq!(r.kind, "error");
        let err = r.error.unwrap_or_default();
        assert!(err.contains("ZeroDivisionError"), "got: {err}");
    }

    #[test]
    fn stderr_is_captured() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        let r = server.eval("import sys\nsys.stderr.write('oops')");
        assert!(r.succeeded());
        assert_eq!(r.stderr, "oops");
    }

    // A raw fd-1 write bypasses `sys.stdout` entirely. It must be captured (not
    // written to the protocol stream), and the NEXT response must still be the
    // correct one — i.e. the stream is not desynchronized.
    #[test]
    fn direct_fd_stdout_write_does_not_corrupt_framing() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        let r = server.eval("import os\nos.write(1, b'raw\\n')");
        assert!(r.succeeded(), "got: {r:?}");
        assert_eq!(r.stdout, "raw\n");
        // Framing intact: the following send gets its own response, not the
        // previous one.
        assert_eq!(server.eval("40 + 2").value.as_deref(), Some("42"));
    }

    // A raw fd-2 write is captured as stderr, not leaked or lost.
    #[test]
    fn direct_fd_stderr_write_is_captured() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        let r = server.eval("import os\nos.write(2, b'boom')");
        assert!(r.succeeded(), "got: {r:?}");
        assert_eq!(r.stderr, "boom");
        assert_eq!(server.eval("1 + 1").value.as_deref(), Some("2"));
    }

    // A subprocess inheriting the eval-server's fds writes to the captures, not
    // the protocol stream. Covers `subprocess.run([...])` without capture, the
    // most common way exploratory REPL code emits raw fd output.
    #[test]
    fn subprocess_output_is_captured_without_breaking_framing() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        let out = server.eval("import subprocess\nsubprocess.run(['printf', 'rawsub'])");
        assert!(out.succeeded(), "got: {out:?}");
        assert!(out.stdout.contains("rawsub"), "stdout: {:?}", out.stdout);
        let err =
            server.eval("import subprocess\nsubprocess.run(['sh', '-c', 'printf oops 1>&2'])");
        assert!(err.succeeded(), "got: {err:?}");
        assert!(err.stderr.contains("oops"), "stderr: {:?}", err.stderr);
        // Two subprocess sends later, framing is still aligned.
        assert_eq!(server.eval("7 * 6").value.as_deref(), Some("42"));
    }

    // Every eval response carries a snapshot of the namespace, because a
    // namespace can only change as the result of a send.
    #[test]
    fn python_reports_its_namespace_on_every_response() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        let bound = server.eval("x = 5\nrows = [1, 2, 3]");
        let vars = bound.vars.unwrap_or_default();
        let x = vars
            .iter()
            .find(|b| b.name == "x")
            .unwrap_or_else(|| panic!("expected x: {vars:?}"));
        assert_eq!(x.kind, "int");
        assert_eq!(x.info.as_deref(), Some("5"));
        let rows = vars.iter().find(|b| b.name == "rows").expect("rows listed");
        assert_eq!(rows.kind, "list");
        assert_eq!(rows.info.as_deref(), Some("len 3"));
        // Interpreter machinery is not a user binding.
        assert!(!vars.iter().any(|b| b.name.starts_with("__")), "{vars:?}");

        // A block that raised part-way through still mutated the namespace, so
        // the error path must snapshot too.
        let failed = server.eval("partial = 1\nraise ValueError('boom')");
        assert!(!failed.succeeded());
        assert!(
            failed
                .vars
                .unwrap_or_default()
                .iter()
                .any(|b| b.name == "partial"),
            "an error response must still report the namespace"
        );
    }

    // Listing what is bound must never execute user code, so a value whose
    // `__repr__` and `__len__` both raise still lists cleanly and cannot fail the
    // send it rode back on.
    #[test]
    fn hostile_bindings_cannot_break_a_send() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        let r = server.eval(
            "class Hostile:\n    def __repr__(self): raise RuntimeError('nope')\n    def __len__(self): raise RuntimeError('nope')\nh = Hostile()",
        );
        assert!(r.succeeded(), "got: {r:?}");
        let vars = r.vars.unwrap_or_default();
        let h = vars
            .iter()
            .find(|b| b.name == "h")
            .unwrap_or_else(|| panic!("expected h: {vars:?}"));
        assert_eq!(h.kind, "Hostile");
        assert_eq!(h.info, None, "a raising __repr__/__len__ yields no info");
        // Framing and the session survive.
        assert_eq!(server.eval("1 + 1").value.as_deref(), Some("2"));
    }

    // --- typescript / bun eval-server (parity + deltas). Each skips when `bun`
    // is absent, mirroring the python3 guard. ---

    // Binding introspection is python-only: the TS eval-server sets no `vars`,
    // because the global lexical environment holding `const`/`let`/`class`/
    // `function` has no enumeration API and a partial list would mislead.
    #[test]
    fn ts_sends_carry_no_bindings() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        let r = server.eval("const a = 1; a");
        assert!(r.succeeded(), "got: {r:?}");
        assert!(r.vars.is_none(), "got: {:?}", r.vars);
    }

    #[test]
    fn ts_state_and_defs_persist_across_requests() {
        let Some(mut server) = EvalServer::start_ts() else {
            eprintln!("skipping: bun not available");
            return;
        };
        // bare, const, function, and class declarations all carry over.
        assert!(server.eval("x = 40").succeeded());
        assert!(server.eval("const y = 2").succeeded());
        assert_eq!(server.eval("x + y").value.as_deref(), Some("42"));
        assert!(server.eval("function dbl(n){ return n * 2 }").succeeded());
        assert_eq!(server.eval("dbl(21)").value.as_deref(), Some("42"));
        assert!(server.eval("class C { get v(){ return 7 } }").succeeded());
        assert_eq!(server.eval("new C().v").value.as_deref(), Some("7"));
    }

    #[test]
    fn ts_trailing_expression_yields_value_statement_does_not() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        // Object/array/null literals survive (deadCodeElimination disabled).
        assert_eq!(server.eval("1 + 2").value.as_deref(), Some("3"));
        assert!(server.eval("({a:1})").value.is_some());
        assert_eq!(server.eval("null").value.as_deref(), Some("null"));
        assert_eq!(server.eval("let z = 5").value, None);
    }

    #[test]
    fn ts_console_and_subprocess_captured_without_breaking_framing() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        let r = server.eval("console.log('hello'); console.log('world')");
        assert!(r.succeeded(), "got: {r:?}");
        assert_eq!(r.stdout, "hello\nworld\n");
        assert_eq!(server.eval("console.error('oops')").stderr, "oops\n");
        // A subprocess inheriting the eval-server's fds lands in the capture, not
        // the protocol stream.
        let sub = server.eval("Bun.spawnSync(['printf', 'rawsub'], { stdout: 'inherit' })");
        assert!(sub.succeeded(), "got: {sub:?}");
        assert!(sub.stdout.contains("rawsub"), "stdout: {:?}", sub.stdout);
        // Framing intact several sends later.
        assert_eq!(server.eval("6 * 7").value.as_deref(), Some("42"));
    }

    #[test]
    fn ts_error_returns_stack() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        let r = server.eval("throw new Error('boom')");
        assert!(!r.succeeded());
        assert_eq!(r.kind, "error");
        assert!(r.error.unwrap_or_default().contains("boom"));
    }

    #[test]
    fn ts_typescript_annotations_are_stripped() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        assert_eq!(
            server.eval("const n: number = 9; n").value.as_deref(),
            Some("9")
        );
    }

    #[test]
    fn ts_top_level_await_bare_assignment_persists_and_notes_declarations() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        // Bare assignment in an awaiting (auto-wrapped) send persists.
        assert!(server.eval("g = await Promise.resolve(100)").succeeded());
        assert_eq!(server.eval("g").value.as_deref(), Some("100"));
        // A `const`/`let`/etc. in an awaiting send does NOT persist, and the
        // response carries a `note` telling the agent to use bare assignment.
        let decl = server.eval("const h = await Promise.resolve(1)");
        assert!(decl.succeeded(), "got: {decl:?}");
        assert!(decl.note.is_some(), "expected a note, got: {decl:?}");
    }

    #[test]
    fn ts_top_level_return_yields_value() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        // Bare top-level return (no await) is retried through the async wrap.
        assert_eq!(server.eval("return 5 + 5").value.as_deref(), Some("10"));
        // Top-level await plus return also yields the returned value.
        let r = server.eval("const os = await import('node:os'); return typeof os.tmpdir");
        assert_eq!(r.value.as_deref(), Some("\"function\""));
    }

    #[test]
    fn ts_static_import_returns_clear_error() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        let r = server.eval("import x from 'node:os'");
        assert!(!r.succeeded());
        let err = r.error.unwrap_or_default();
        assert!(err.contains("require"), "got: {err}");
        // The server survives the rejected import and keeps framing.
        assert_eq!(server.eval("1 + 1").value.as_deref(), Some("2"));
    }

    #[test]
    fn ts_user_thrown_syntaxerror_does_not_double_execute() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        assert!(server.eval("count = 0").succeeded());
        // A user-thrown SyntaxError must NOT be mistaken for a top-level-return
        // parse error and retried (which would run the side effect twice).
        let r = server.eval("count++; throw new SyntaxError('nope')");
        assert!(!r.succeeded());
        assert_eq!(server.eval("count").value.as_deref(), Some("1"));
    }

    #[test]
    fn ts_malformed_request_is_framed_error() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        let r = server.send_raw("not json at all");
        assert!(!r.succeeded());
        assert!(r.error.unwrap_or_default().contains("malformed"));
        // Still alive and framing correctly.
        assert_eq!(server.eval("2 + 2").value.as_deref(), Some("4"));
    }
}
