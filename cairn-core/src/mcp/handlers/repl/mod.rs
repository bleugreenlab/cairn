//! Stateful REPL host.
//!
//! A REPL is a persistent interpreter subprocess (an "eval-server") that reads
//! JSON-lines requests on stdin and writes JSON-lines responses on stdout,
//! holding a live namespace across `run` calls. The live child handles live in
//! an in-memory, node-scoped registry on the orchestrator ([`ReplState`],
//! mirroring `pty_state`) with **no database table**: a REPL is explicitly
//! non-durable, so a DB row would advertise a live session whose process no
//! longer exists. The registry is the single source of truth; create, read,
//! delete, and teardown all consult it.
//!
//! Lifetime = job/worktree lifetime: the always-on orchestrator owns the child,
//! so a REPL survives intra-execution turn suspends (the whole point — state
//! persisting across `run` calls that span turns) and is killed at node/worktree
//! teardown (the hard guarantee against orphans). On host restart the registry
//! is empty, so a prior slug resolves as *unknown* (recreate it), never "died".

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};

use crate::mcp::handlers::RunContext;
use crate::orchestrator::Orchestrator;
use cairn_common::executor_protocol::{
    CellOccupant, CellPriority, LifetimeLeaseAcquireRequest, LifetimeLeaseDeclaration,
    LifetimeLeaseFence, LifetimeLeaseOperation, LifetimeLeaseOwnerKind, LifetimeLeaseResult,
    LifetimeOwnerDeathPolicy, LifetimeProcessCwdRoot, LifetimeProcessEventKind,
    LifetimeProcessIoMode, LifetimeProcessSpec, LifetimeProcessStatus, LifetimeProcessStream,
    LifetimeRuntimeAsset, ProcessSandboxMode, RepositoryLocator, ResourceReservation,
    ResourceReservationSource,
};

/// Max exchanges retained per session (bounded ring; oldest evicted first).
const HISTORY_CAP: usize = 200;
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

/// One recorded request/response pair in a session's in-memory history ring.
/// Emitted over `repl-exchange` (phase `started` when appended pending, `settled`
/// when the outcome lands) and replayed to a newly-opened REPL tab via
/// `get_repl_history`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplExchange {
    pub seq: u64,
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

/// Registry-listing view of a live session, for facet projection and the tab
/// header. Carries no exchange history (that comes from `get_repl_history`).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplInfo {
    pub job_id: String,
    pub slug: String,
    pub interpreter: String,
    /// Epoch milliseconds at spawn.
    pub created_at: i64,
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

/// A live eval-server session held behind an `Arc` in [`ReplState`].
pub struct ReplSession {
    pub(crate) interpreter: ReplLang,
    fence: LifetimeLeaseFence,
    process_key: String,
    process_generation: u64,
    responses: Mutex<mpsc::Receiver<String>>,
    alive: AtomicBool,
    created_at: SystemTime,
    /// Serializes request->response round-trips so two `run` items targeting the
    /// same slug (items run in parallel by default) cannot interleave on the
    /// single-threaded eval-server. Different slugs stay concurrent.
    send_lock: tokio::sync::Mutex<()>,
    /// Monotonic exchange sequence, shared across agent and user sends.
    seq: AtomicU64,
    /// Bounded, in-memory transcript of this session's exchanges. Non-durable by
    /// design (a host restart clears the whole registry), consistent with the
    /// REPL contract; capped at [`HISTORY_CAP`] with oldest-first eviction.
    history: Mutex<VecDeque<ReplExchange>>,
}

impl ReplSession {
    pub async fn stop_and_release(&self, orch: &Orchestrator) {
        self.alive.store(false, Ordering::Release);
        let _ = crate::fleet::lifetime::stop(orch, &self.fence, &self.process_key).await;
        let _ = crate::fleet::lifetime::release(orch, &self.fence).await;
    }

    /// True while the child is still running (non-blocking).
    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    /// True while a send holds `send_lock` (an exchange is in flight).
    pub fn is_busy(&self) -> bool {
        self.send_lock.try_lock().is_err()
    }

    /// Allocate the next exchange sequence number.
    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Append a (pending) exchange, evicting the oldest past the cap.
    fn push_history(&self, exchange: ReplExchange) {
        if let Ok(mut history) = self.history.lock() {
            history.push_back(exchange);
            while history.len() > HISTORY_CAP {
                history.pop_front();
            }
        }
    }

    /// Replace the pending exchange carrying `seq` with its settled form. A no-op
    /// if the pending entry was already evicted (only at extreme depth).
    fn settle_history(&self, seq: u64, settled: ReplExchange) {
        if let Ok(mut history) = self.history.lock() {
            if let Some(slot) = history.iter_mut().find(|e| e.seq == seq) {
                *slot = settled;
            }
        }
    }

    /// Snapshot the current transcript (oldest first) for a newly-opened tab.
    pub fn history_snapshot(&self) -> Vec<ReplExchange> {
        self.history
            .lock()
            .map(|h| h.iter().cloned().collect())
            .unwrap_or_default()
    }
}

/// In-memory, node-scoped registry of live REPL sessions, keyed by
/// `(job_id, slug)`. One instance lives on the orchestrator, shared by every
/// host exactly like `pty_state`.
#[derive(Default)]
pub struct ReplState {
    sessions: Mutex<HashMap<(String, String), Arc<ReplSession>>>,
}

impl ReplState {
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
        keys.into_iter()
            .filter_map(|(job_id, slug)| {
                sessions
                    .remove(&(job_id.clone(), slug.clone()))
                    .map(|session| (job_id, slug, session))
            })
            .collect()
    }

    /// Live-session listing for one job (facet projection + tab list).
    pub fn list_for_job(&self, job_id: &str) -> Vec<ReplInfo> {
        let Ok(sessions) = self.sessions.lock() else {
            return Vec::new();
        };
        sessions
            .iter()
            .filter(|((jid, _), _)| jid == job_id)
            .map(|((jid, slug), session)| repl_info(jid, slug, session))
            .collect()
    }

    /// Live-session listing across every job on this host (global facet source).
    pub fn list_all(&self) -> Vec<ReplInfo> {
        let Ok(sessions) = self.sessions.lock() else {
            return Vec::new();
        };
        sessions
            .iter()
            .map(|((jid, slug), session)| repl_info(jid, slug, session))
            .collect()
    }
}

fn repl_info(job_id: &str, slug: &str, session: &Arc<ReplSession>) -> ReplInfo {
    let created_at = session
        .created_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    ReplInfo {
        job_id: job_id.to_string(),
        slug: slug.to_string(),
        interpreter: session.interpreter.label().to_string(),
        created_at,
        alive: session.is_alive(),
        busy: session.is_busy(),
    }
}

/// Start an eval server in an executor lifetime lease.
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
    let branch = branch
        .or(base_branch)
        .ok_or_else(|| "REPL job has no logical branch".to_string())?;
    let tip = crate::fleet::lifetime::resolve_logical_commit(
        orch,
        std::path::Path::new(&repo_path),
        &branch,
    )
    .await?;

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
    let policy = crate::mcp::handlers::run::build_run_sandbox_policy(
        orch,
        cwd,
        run_context.map(|c| c.run_id.as_str()),
        Some(project_id),
        None,
    )
    .await
    .map(|(p, _)| p);
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
            .map(|p| cairn_common::executor_protocol::LifetimeSandboxPolicy {
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
    let owner =
        crate::fleet::lifetime::owner(LifetimeLeaseOwnerKind::Repl, format!("{job_id}:{slug}"));
    let request = LifetimeLeaseAcquireRequest {
        declaration: LifetimeLeaseDeclaration {
            lease_id: format!("repl:{job_id}:{slug}"),
            owner,
            owner_ref: None,
            name: slug.into(),
            purpose: "stateful REPL".into(),
            repository: RepositoryLocator::ColocatedPath {
                project_id: project_id.into(),
                repository_id: project_id.into(),
                absolute_path: repo_path,
            },
            initial_base_commit: tip.clone(),
            resource_reservation: ResourceReservation {
                memory_bytes: 268435456,
                disk_growth_bytes: 536870912,
                concurrency_units: 1,
                source: ResourceReservationSource::Declared,
            },
            owner_death_policy: LifetimeOwnerDeathPolicy {
                heartbeat_timeout_ms: 180000,
                reclaim_grace_ms: 30000,
            },
        },
        priority: CellPriority::AgentInteractive,
        deadline_unix_ms: now_millis() as u64 + 30000,
    };
    let fence = crate::fleet::lifetime::acquire(orch, request).await?;
    if let Err(e) = crate::fleet::lifetime::refresh(orch, &fence, &tip).await {
        let _ = crate::fleet::lifetime::release(orch, &fence).await;
        return Err(e);
    }

    let key = "eval-server".to_string();
    let (tx, rx) = mpsc::sync_channel(2);
    let buffer = Arc::new(Mutex::new(Vec::<u8>::new()));
    let early_alive = Arc::new(AtomicBool::new(true));
    let ef = fence.clone();
    let ek = key.clone();
    let eb = buffer.clone();
    let ea = early_alive.clone();
    orch.fleet.subscribe_lifetime_process_events(move |event| {
        if event.lease_id != ef.lease_id
            || event.incarnation_id != ef.incarnation_id
            || event.lease_epoch != ef.lease_epoch
            || event.process_key != ek
        {
            return;
        }
        match event.event {
            LifetimeProcessEventKind::Output {
                stream: LifetimeProcessStream::Stdout,
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
            LifetimeProcessEventKind::Output {
                stream: LifetimeProcessStream::Stderr,
                data,
                ..
            } => tracing::debug!(diagnostic=%String::from_utf8_lossy(&data),"REPL stderr"),
            LifetimeProcessEventKind::State {
                status: LifetimeProcessStatus::Exited { .. },
            } => ea.store(false, Ordering::Release),
            _ => {}
        }
    });
    let started = orch
        .fleet
        .operate_lifetime_lease(
            orch,
            LifetimeLeaseOperation::StartProcess {
                fence: fence.clone(),
                process_key: key.clone(),
                process: LifetimeProcessSpec {
                    program,
                    args,
                    cwd: String::new(),
                    cwd_root: LifetimeProcessCwdRoot::Checkout,
                    env,
                    sandbox_mode: if sandbox_policy.is_some() {
                        ProcessSandboxMode::Confined
                    } else {
                        ProcessSandboxMode::Unconfined
                    },
                    sandbox_policy,
                    runtime_assets: vec![LifetimeRuntimeAsset {
                        path: asset.into(),
                        data: body.as_bytes().to_vec(),
                    }],
                    io: LifetimeProcessIoMode::Pipe,
                },
            },
        )
        .await;
    let LifetimeLeaseResult::State { cell } = started else {
        let _ = crate::fleet::lifetime::release(orch, &fence).await;
        return Err(format!("failed to start REPL: {started:?}"));
    };
    let Some(generation) = cell
        .occupant
        .as_ref()
        .and_then(CellOccupant::lifetime)
        .and_then(|l| l.processes.get(&key))
        .map(|p| p.generation)
    else {
        crate::fleet::lifetime::rollback(orch, &fence, &key).await;
        return Err("REPL start returned no process generation".to_string());
    };
    let session = Arc::new(ReplSession {
        interpreter,
        fence: fence.clone(),
        process_key: key,
        process_generation: generation,
        responses: Mutex::new(rx),
        alive: AtomicBool::new(early_alive.load(Ordering::Acquire)),
        created_at: SystemTime::now(),
        send_lock: tokio::sync::Mutex::new(()),
        seq: AtomicU64::new(0),
        history: Mutex::new(VecDeque::new()),
    });
    let weak = Arc::downgrade(&session);
    let xf = fence.clone();
    let xk = session.process_key.clone();
    orch.fleet.subscribe_lifetime_process_events(move |e| {
        if e.lease_id == xf.lease_id
            && e.incarnation_id == xf.incarnation_id
            && e.lease_epoch == xf.lease_epoch
            && e.process_key == xk
            && e.process_generation == generation
            && matches!(
                e.event,
                LifetimeProcessEventKind::State {
                    status: LifetimeProcessStatus::Exited { .. }
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
            if crate::fleet::lifetime::renew(&ro, &rf).await.is_err() {
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
        .operate_lifetime_lease(
            orch,
            LifetimeLeaseOperation::WriteProcessInput {
                fence: session.fence.clone(),
                process_key: session.process_key.clone(),
                process_generation: session.process_generation,
                data,
            },
        )
        .await;
    if !matches!(result, LifetimeLeaseResult::State { .. }) {
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

/// Emit a `repl-exchange` event (phase `started` on submit, `settled` on
/// outcome) through the orchestrator's generic emitter, which reaches the
/// desktop over Tauri and cairn-server over its WS bridge with no per-event
/// wiring.
fn emit_exchange(
    orch: &Orchestrator,
    job_id: &str,
    slug: &str,
    phase: &str,
    exchange: &ReplExchange,
) {
    let _ = orch.services.emitter.emit(
        "repl-exchange",
        serde_json::json!({
            "jobId": job_id,
            "slug": slug,
            "seq": exchange.seq,
            "phase": phase,
            "origin": exchange.origin,
            "code": exchange.code,
            "status": exchange.status,
            "value": exchange.value,
            "stdout": exchange.stdout,
            "stderr": exchange.stderr,
            "error": exchange.error,
            "note": exchange.note,
            "durationMs": exchange.duration_ms,
            "truncated": exchange.truncated,
        }),
    );
}

/// Emit a `repl-state` lifecycle event (`created` | `exited` | `deleted`).
pub(crate) fn emit_repl_state(
    orch: &Orchestrator,
    job_id: &str,
    slug: &str,
    interpreter: ReplLang,
    status: &str,
) {
    let _ = orch.services.emitter.emit(
        "repl-state",
        serde_json::json!({
            "jobId": job_id,
            "slug": slug,
            "interpreter": interpreter.label(),
            "status": status,
        }),
    );
}

/// The one canonical send funnel: record a pending exchange, run the send,
/// perform any Dead/Timeout kill-and-unregister, settle the history entry, and
/// emit the `started`/`settled` `repl-exchange` events (plus a `repl-state`
/// `exited` on a session-ending outcome). Both the agent path (`run` item) and
/// the user path (REPL tab composer) route through here so every exchange is
/// recorded and broadcast identically.
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
            "No REPL named '{slug}' for this node. Create it: write cairn:~/repl/{slug} {{interpreter:\"python\"|\"typescript\"}}"
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

    let seq = session.next_seq();
    let mut exchange = ReplExchange {
        seq,
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
    session.push_history(exchange.clone());
    emit_exchange(orch, job_id, slug, "started", &exchange);

    let started = Instant::now();
    let result = send(orch, &session, code, timeout).await;
    exchange.duration_ms = Some(started.elapsed().as_millis() as u64);

    let mut session_ended = false;
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
        }
        ReplSendResult::Dead => {
            // Unregister only if this is still the live session: a close+recreate
            // during the send may have installed a new generation under this slug,
            // which this obsolete outcome must not evict.
            session_ended = orch.repl_state.remove_if(job_id, slug, &session);
            session.stop_and_release(orch).await;
            exchange.status = ReplExchangeStatus::Died;
            exchange.error = Some(format!("REPL '{slug}' died — state lost; recreate it."));
        }
        ReplSendResult::Timeout => {
            // Kill the timed-out child we hold, but unregister only when it is
            // still the registered session, so a replacement generation created
            // during the send is never removed or killed by this stale outcome.
            session_ended = orch.repl_state.remove_if(job_id, slug, &session);
            session.stop_and_release(orch).await;
            exchange.status = ReplExchangeStatus::Timeout;
            exchange.error = Some(format!(
                "REPL '{slug}' send timed out after {}ms; the REPL was killed and its state lost. Recreate it and break long-running work into smaller sends.",
                timeout.as_millis()
            ));
        }
        ReplSendResult::Protocol(message) => {
            session_ended = orch.repl_state.remove_if(job_id, slug, &session);
            session.stop_and_release(orch).await;
            exchange.status = ReplExchangeStatus::Protocol;
            exchange.error = Some(format!("{message}; REPL state lost, recreate it."));
        }
    }

    session.settle_history(seq, exchange.clone());
    emit_exchange(orch, job_id, slug, "settled", &exchange);
    if session_ended {
        emit_repl_state(orch, job_id, slug, session.interpreter, "exited");
    }
    Ok(exchange)
}

/// Render a REPL read: a one-line status banner from the live registry.
pub(crate) fn render_status(slug: &str, session: Option<&Arc<ReplSession>>) -> String {
    match session {
        None => format!(
            "[repl {slug}: not found] No REPL named '{slug}' for this node. \
             Create it: write cairn:~/repl/{slug} {{interpreter:\"python\"|\"typescript\"}}"
        ),
        Some(session) => {
            let alive = session.is_alive();
            let uptime = session
                .created_at
                .elapsed()
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let state = if alive { "running" } else { "exited" };
            format!(
                "[repl {slug}: {}, {state}, up {uptime}s]",
                session.interpreter.label()
            )
        }
    }
}

/// Resolve the top-level node job id for a `NodeRepl` URI's coordinates,
/// mirroring the terminal target lookup. Used by read and delete to reach the
/// registry key from URI coordinates.
pub(crate) async fn resolve_node_repl_job_id(
    db: &crate::storage::LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
) -> Option<String> {
    use crate::storage::RowExt;
    let project_key = project_key.to_uppercase();
    let node_id = node_id.to_string();
    db.read(|conn| {
        let project_key = project_key.clone();
        let node_id = node_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "
                    SELECT j.id
                    FROM jobs j
                    JOIN issues i ON j.issue_id = i.id
                    JOIN projects p ON i.project_id = p.id
                    JOIN executions e ON j.execution_id = e.id
                    WHERE p.key = ?1
                      AND i.number = ?2
                      AND e.seq = ?3
                      AND j.parent_job_id IS NULL
                      AND j.uri_segment = ?4
                    LIMIT 1
                    ",
                    (project_key.as_str(), number, exec_seq, node_id.as_str()),
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(row.text(0)?)),
                None => Ok(None),
            }
        })
    })
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

    // --- typescript / bun eval-server (parity + deltas). Each skips when `bun`
    // is absent, mirroring the python3 guard. ---

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
