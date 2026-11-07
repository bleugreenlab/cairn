//! Per-run bearer tokens: the credential every agent process this runner
//! spawns presents to its MCP callback.
//!
//! # Why a bearer per run
//!
//! The credential itself has to say which run is calling, because whatever a
//! request body claims is chosen by whoever holds the credential. A run bearer
//! does: it identifies exactly one run, the endpoint attributes every call to
//! that run from the token alone ([`bind_request_to_run`]), and it stops
//! working when the run finalizes. Both attachments carry one: the stdio shim
//! (`cairn-cmd mcp`, which posts to the runner's loopback `/api/mcp`) and the
//! streamable-HTTP endpoint. The machine's MCP secret speaks for no run at all.
//!
//! # The one bearer that spans runs
//!
//! A pooled Codex app-server hosts many runs' ephemeral calls behind one
//! `cairn-cmd`, so no single run's bearer can be its credential. Its bearer is
//! scoped to that one server instead ([`BearerScope::CodexPool`]): a request
//! must name a thread that is a live call on that server, and is attributed to
//! that call's run ([`bind_request_to_pool_thread`]). It is revoked when the
//! server closes. This is the only credential whose authority covers more than
//! one run, and it is bounded by the server's own calls: a call stops counting
//! the moment its run finalizes, though the bearer lives on for its neighbours.
//!
//! # Why the book is process-local
//!
//! Bearers are leases (see [`crate::security::lease`]) and share the lease
//! book's rule: they are never persisted. A restart therefore drops every
//! bearer, which is exactly the host-affinity rule the stdio path enforces by
//! other means: a tool call is served only by the process that spawned its run,
//! because a successor process has no transcript reader for it. A bearer from a
//! predecessor, or from another orchestrator entirely, is simply unknown here.
//!
//! # What the book holds
//!
//! The SHA-256 digest of each token, never the token. The token's plaintext
//! lives only inside its lease, from which it is presented into the process that
//! carries it (an agent's or a project service's environment, a service call's
//! frame, or a dev export) and compared in constant time on each request.
//! A revoked bearer leaves a non-secret tombstone for a day so a later
//! presentation can be told *why* it is refused rather than just that it is.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use rand::RngCore;
use sha2::{Digest, Sha256};

use cairn_models::session::mcp::{McpServer, McpTransport, MCP_TOKEN_ENV};

use crate::orchestrator::Orchestrator;
use crate::security::broker::CredentialSource;
use crate::security::lease::{
    leases, CredentialLease, LeaseAudience, LeaseDenied, LeaseId, LeaseTerms, Presented,
};

/// Where agents reach this orchestrator's `/mcp`: the workspace's configured
/// `mcp.publicUrl` (for a headless orch, typically its `tailscale serve`
/// hostname), else this machine's loopback.
pub fn public_mcp_url(orch: &Orchestrator) -> String {
    crate::config::settings::load_mcp_public_url(&orch.config_dir)
        .unwrap_or_else(|| format!("http://127.0.0.1:{}/mcp", orch.mcp_callback_port))
}

/// The streamable-HTTP attachment for one run, with a bearer freshly minted
/// for it and bound to the attachment's host.
pub fn attachment_for_run(
    orch: &Orchestrator,
    run_id: &str,
    node_uri: &str,
) -> Result<(McpServer, RunBearer), String> {
    let attachment = McpServer::cairn_http(public_mcp_url(orch));
    let bearer = bearer_for_attachment(orch, &attachment, run_id, node_uri)?;
    Ok((attachment, bearer))
}

/// A bearer for `run_id`, bound to the destination `attachment` reaches: the
/// public endpoint's host for HTTP, this machine's loopback for the stdio
/// shim.
pub fn bearer_for_attachment(
    orch: &Orchestrator,
    attachment: &McpServer,
    run_id: &str,
    node_uri: &str,
) -> Result<RunBearer, String> {
    orch.run_bearers
        .mint(run_id, node_uri, attachment_audience(attachment)?)
        .map_err(|refusal| refusal.to_string())
}

/// A bearer for the pooled Codex server `server_id`, bound to the destination
/// `attachment` reaches. See the module docs on why it alone spans runs.
pub fn bearer_for_pool(
    orch: &Orchestrator,
    attachment: &McpServer,
    server_id: &str,
) -> Result<RunBearer, String> {
    Ok(orch
        .run_bearers
        .mint_pool(server_id, attachment_audience(attachment)?))
}

/// The variable `cairn-cmd` reads its callback credential from. The name
/// predates per-run bearers; what it carries is now always a bearer.
pub const STDIO_CREDENTIAL_ENV: &str = "CAIRN_MCP_SECRET";

/// Where `attachment`'s agent sends the bearer it presents: the HTTP
/// endpoint, or the loopback callback the stdio shim posts to.
fn attachment_destination(attachment: &McpServer) -> Result<&str, String> {
    match &attachment.transport {
        McpTransport::Http { url, .. } => Ok(url),
        McpTransport::Stdio { env, .. } => env
            .get("CAIRN_CALLBACK_URL")
            .map(String::as_str)
            .ok_or_else(|| "a stdio MCP attachment names no CAIRN_CALLBACK_URL".to_string()),
    }
}

fn attachment_audience(attachment: &McpServer) -> Result<LeaseAudience, String> {
    audience_for_url(attachment_destination(attachment)?)
}

/// The environment variable, and its value, that an agent process's MCP entry
/// authenticates with: its bearer, under the name the stdio shim or the HTTP
/// client reads it from.
pub fn mcp_credential_env(
    attachment: &McpServer,
    bearer: &RunBearer,
) -> Result<(&'static str, String), String> {
    let var = match &attachment.transport {
        McpTransport::Stdio { .. } => STDIO_CREDENTIAL_ENV,
        McpTransport::Http { .. } => MCP_TOKEN_ENV,
    };
    // Presented to the agent that will carry it to the attachment's
    // destination, so the audience is derived from where it is going.
    let presented = bearer
        .present(&attachment_audience(attachment)?)
        .map_err(|denied| format!("run bearer not presentable: {denied}"))?;
    Ok((var, presented.expose().to_string()))
}

/// The value a project service process presents to `audience`, where it
/// carries its callbacks: its standing run's bearer, set in its environment at
/// launch, or a call's delegated bearer, written into that call's frame on its
/// stdin.
///
/// The service counterpart of [`mcp_credential_env`]. The supervisor holds each
/// bearer as a lease and calls this as it builds what it writes to the
/// process, so the value is materialised for that write and no earlier. The
/// audience is this runner's loopback for a service on its machine, or
/// [`relay_audience`] of the executor a service elsewhere is relayed through.
pub fn service_credential(bearer: &RunBearer, audience: &LeaseAudience) -> Result<String, String> {
    let presented = bearer
        .present(audience)
        .map_err(|denied| format!("run bearer not presentable: {denied}"))?;
    Ok(presented.expose().to_string())
}

/// The audience of a bearer a project service on executor `executor_id`
/// presents at that executor's resident relay.
///
/// A role of its own, never the loopback role: the relay's URL is loopback on
/// the executor's machine, and a bearer bound to the runner's loopback would
/// otherwise be presentable at every relay, and a relay's at the runner.
pub fn relay_audience(executor_id: &str) -> LeaseAudience {
    LeaseAudience::process(format!("executor-relay:{executor_id}"))
}

/// Hold a callback request to the one run its credential speaks for: a run it
/// names must be this one, and an absent run is filled with it. A thread it names
/// is dropped, not refused. Codex stamps `_meta.threadId` on every tool call,
/// pooled or not, so a per-run session always sends one, and left in place it
/// would re-attribute the request to whichever run holds that thread.
///
/// The rule every per-run credential gets, whether it arrived as a bearer at
/// `/api/mcp` or through an executor's resident relay. [`bind_request`]
/// applies it, or the pool rule, by the bearer's scope.
pub fn bind_request_to_run(
    request: &mut crate::mcp::types::McpCallbackRequest,
    run_id: &str,
) -> Result<(), String> {
    request.thread_id = None;
    match request
        .run_id
        .as_deref()
        .filter(|claimed| !claimed.is_empty())
    {
        Some(claimed) if claimed != run_id => Err(format!(
            "this credential speaks for run {run_id}, and the request claims run {claimed}"
        )),
        _ => {
            request.run_id = Some(run_id.to_string());
            Ok(())
        }
    }
}

/// The `CAIRN_RUN_ID` a pooled Codex server's `cairn-cmd` carries. It names no
/// run: every pooled call is attributed by its thread.
pub const POOLED_CODEX_RUN_SENTINEL: &str = "codex-pooled-app-server";

/// Hold a pooled Codex server's callback to one of that server's own calls.
///
/// The request must name a thread that is a live call on server `server_id`;
/// it is attributed to that call's run. A run it names besides the sentinel
/// must be that run. A thread on another server, a closed server, or no
/// thread at all is refused.
///
/// So is a thread whose run has been revoked. Finalization also removes the
/// binding, but a call that registers while its run finalizes can land after
/// that removal, and the book is what says the run is over.
pub fn bind_request_to_pool_thread(
    pools: &crate::backends::codex::pool::CodexAppServerPool,
    bearers: &RunBearerBook,
    request: &mut crate::mcp::types::McpCallbackRequest,
    server_id: &str,
) -> Result<(), String> {
    let Some(thread_id) = request
        .thread_id
        .as_deref()
        .filter(|thread| !thread.is_empty())
    else {
        return Err(format!(
            "this credential speaks for the calls on pooled Codex server {server_id}, and the \
             request names none of its threads"
        ));
    };
    let Some(binding) = pools.binding_for_server_thread(server_id, thread_id) else {
        return Err(format!(
            "thread {thread_id} is not a live call on pooled Codex server {server_id}"
        ));
    };
    if bearers.run_is_revoked(&binding.run_id) {
        return Err(format!(
            "thread {thread_id} belongs to run {}, which has finalized",
            binding.run_id
        ));
    }
    match request
        .run_id
        .as_deref()
        .filter(|claimed| !claimed.is_empty() && *claimed != POOLED_CODEX_RUN_SENTINEL)
    {
        Some(claimed) if claimed != binding.run_id => Err(format!(
            "thread {thread_id} belongs to run {}, and the request claims run {claimed}",
            binding.run_id
        )),
        _ => {
            request.run_id = Some(binding.run_id);
            Ok(())
        }
    }
}

/// Hold a request to what its verified bearer speaks for.
pub fn bind_request(
    orch: &Orchestrator,
    request: &mut crate::mcp::types::McpCallbackRequest,
    scope: &BearerScope,
) -> Result<(), String> {
    match scope {
        BearerScope::Run(run_id) => bind_request_to_run(request, run_id),
        BearerScope::CodexPool(server_id) => {
            bind_request_to_pool_thread(&orch.codex_pool, &orch.run_bearers, request, server_id)
        }
    }
}

/// The callback tools a request that speaks for no run may call. Every other
/// tool needs a credential that names a run, and these read `cairn://` resources
/// alone (`handlers::read::batch` refuses such a request any host file or web
/// target). Adding a tool widens what a local process can do with no run to
/// answer for it, so `the_run_less_allowlist_is_pinned` makes that a reviewed
/// change.
const RUN_LESS_TOOLS: &[&str] = &[
    "read_batch",
    "read_resource",
    "read_issue_resource",
    "watch",
    "process_info",
];

/// `request`, when it is one `write` of one append and nothing else: no commit
/// message, branch or other batch option. A typed client serializes its unset
/// options as null (or `false`), so those count as absent.
fn lone_append(
    request: &crate::mcp::types::McpCallbackRequest,
) -> Option<(cairn_uri::CairnResource, &serde_json::Value)> {
    if request.tool != "write" {
        return None;
    }
    let batch = request.payload.as_object()?;
    let unset = |key: &str, value: &serde_json::Value| {
        key == "changes"
            // The CLI's stamp of what kind of caller it is. It grants nothing.
            || key == cairn_common::protocol::CALLBACK_ORIGIN_KEY
            || value.is_null()
            || (key != "commit_msg" && *value == serde_json::Value::Bool(false))
    };
    if !batch.iter().all(|(key, value)| unset(key, value)) {
        return None;
    }
    let [change] = batch.get("changes")?.as_array()?.as_slice() else {
        return None;
    };
    let change = change.as_object()?;
    if change
        .keys()
        .any(|key| !matches!(key.as_str(), "target" | "mode" | "payload"))
        || change.get("mode")?.as_str()? != "append"
    {
        return None;
    }
    let target = cairn_uri::parse_uri(change.get("target")?.as_str()?)?;
    Some((target, change.get("payload")?))
}

/// `request`'s target, when it is one append of `{content}` and nothing else.
pub(crate) fn lone_content_append(
    request: &crate::mcp::types::McpCallbackRequest,
) -> Option<cairn_uri::CairnResource> {
    let (target, payload) = lone_append(request)?;
    let payload = payload.as_object()?;
    (payload.len() == 1 && payload.get("content").is_some_and(|c| c.is_string())).then_some(target)
}

/// A thread's, an issue's, a node's or a task's `/messages` collection. Exact, so
/// a table a thread's database happens to name `messages` is not one.
fn is_message_collection(target: &cairn_uri::CairnResource) -> bool {
    use cairn_uri::CairnResource as R;
    match target {
        R::IssueMessages { .. } | R::NodeMessages { .. } | R::TaskMessages { .. } => true,
        R::Thread { path, .. } => matches!(
            path.iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .as_slice(),
            ["messages"] | ["task", _, "messages"]
        ),
        _ => false,
    }
}

/// Hold a request that speaks for no run to the run-less allowlist, and mark it
/// as such. It may claim no run. It reaches [`RUN_LESS_TOOLS`]; the delivery of a
/// pending self-rebuild outcome, the one write with no run left to make it; and
/// a message to a `/messages` collection, because a local agent outside Cairn has
/// no other way in. That message is labelled as unverified wherever it is stored
/// or shown, since nothing proves who sent it.
fn hold_run_less(
    request: &mut crate::mcp::types::McpCallbackRequest,
    holder: &str,
    also_admits: impl Fn(&crate::mcp::types::McpCallbackRequest) -> bool,
) -> Result<(), String> {
    // Codex stamps a thread on every call, and only a pool credential resolves one.
    request.thread_id = None;
    if let Some(claimed) = request
        .run_id
        .as_deref()
        .filter(|claimed| !claimed.is_empty())
    {
        return Err(format!(
            "{holder} speaks for no run, and the request claims run {claimed}"
        ));
    }
    let admitted = RUN_LESS_TOOLS.contains(&request.tool.as_str())
        || crate::resources::self_rebuild::is_self_rebuild_delivery(request)
        || lone_content_append(request).is_some_and(|target| is_message_collection(&target))
        || also_admits(request);
    if !admitted {
        return Err(format!(
            "{holder} speaks for no run: it can read cairn:// resources, watch, and post a \
             message to a /messages collection, not make this `{}` call. Make it from a Cairn \
             run, or as the operator in the desktop app",
            request.tool
        ));
    }
    request.run_id = None;
    request.origin = Some(cairn_common::protocol::DispatchOrigin::UnverifiedLocal);
    Ok(())
}

/// Hold a callback request to what the machine credential may do.
pub fn hold_to_machine_credential(
    request: &mut crate::mcp::types::McpCallbackRequest,
) -> Result<(), String> {
    hold_run_less(request, "the machine credential", |_| false)
}

/// Hold a call from a resident process started for no run, such as a project
/// terminal, to the same allowlist. A process Cairn started may also ask for a
/// dev-instance launch ticket, which is how `bun dev:instance` launches from a
/// project terminal; the machine credential has no use for one.
pub fn hold_to_run_less_process(
    request: &mut crate::mcp::types::McpCallbackRequest,
) -> Result<(), String> {
    hold_run_less(request, "this process", |request| {
        lone_append(request).is_some_and(|(target, payload)| {
            matches!(target, cairn_uri::CairnResource::DevLaunchTicket)
                && payload
                    .as_object()
                    .is_some_and(|payload| payload.is_empty())
        })
    })
}

/// The consumer role a bearer is bound to when the endpoint is reached over
/// loopback, where there is no hostname to bind it to.
const AGENT_MCP_ROLE: &str = "agent-mcp";

/// How long a bearer stays presentable without its run finalizing. Finalization
/// is the real end of a bearer; this only bounds one whose run never reaches it.
const BEARER_LIFETIME_SECS: i64 = 30 * 24 * 60 * 60;

/// How long a revoked or expired bearer's tombstone is kept for refusal text.
const TOMBSTONE_RETENTION_SECS: i64 = 24 * 60 * 60;

/// Distinguishes every bearer minted in this process, so each registers its own
/// scrubbing identity. Process-global rather than per book: two orchestrators in
/// one process must still never register two values under one identity, which
/// would stop the first value being scrubbed.
static BEARER_SERIAL: AtomicU64 = AtomicU64::new(0);

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// The audience a bearer presented at `host` (a `Host` header value, with or
/// without a port) is checked against. Loopback has no name to bind to and maps
/// to the local agent role; any other host is an HTTPS destination.
pub fn audience_for_host(host: &str) -> LeaseAudience {
    let host = strip_port(host.trim());
    let unbracketed = host.trim_start_matches('[').trim_end_matches(']');
    let loopback = unbracketed.eq_ignore_ascii_case("localhost")
        || unbracketed
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    if loopback {
        LeaseAudience::process(AGENT_MCP_ROLE)
    } else {
        LeaseAudience::https(host)
    }
}

/// The audience a bearer for the endpoint at `public_url` is minted for: the
/// same derivation [`audience_for_host`] applies to the requests that arrive
/// there, so a bearer is presentable only through the address it was issued
/// for.
pub fn audience_for_url(public_url: &str) -> Result<LeaseAudience, String> {
    let parsed = url::Url::parse(public_url)
        .map_err(|error| format!("MCP public URL {public_url:?} is not a URL: {error}"))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| format!("MCP public URL {public_url:?} names no host"))?;
    Ok(audience_for_host(host))
}

/// What a dev export wrote: where, and under which lease. Never the token.
#[cfg(debug_assertions)]
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DevAttachmentExport {
    pub run_id: String,
    pub lease_id: String,
    pub url: String,
    /// `CAIRN_MCP_TOKEN=<bearer>`, for `source`-ing on the agent's host.
    pub token_env: std::path::PathBuf,
    /// The Claude `--mcp-config` JSON (names the bearer by variable only).
    pub claude_mcp_config: std::path::PathBuf,
    /// The Codex `-c` override value (names the bearer by variable only).
    pub codex_config_override: std::path::PathBuf,
}

/// Development only: mint an HTTP attachment for a live run and write it to
/// `out_dir`, so a `claude` or `codex` on another host can be pointed at this
/// orchestrator by hand. The directory is owner-only and so is each file; the
/// bearer is revoked with the run like any other.
#[cfg(debug_assertions)]
pub async fn dev_export_attachment(
    orch: &Orchestrator,
    run_id: &str,
    out_dir: &std::path::Path,
) -> Result<DevAttachmentExport, String> {
    use crate::storage::{DbError, RowExt};

    // A bearer minted for a run that has already finalized would never be
    // revoked, so only a live run's attachment may be exported.
    let db = crate::execution::routing::routing_db_for_id(&orch.db, run_id)
        .await
        .map_err(|error| error.to_string())?;
    let lookup = run_id.to_string();
    let status = db
        .read(|conn| {
            let run_id = lookup.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT status FROM runs WHERE id = ?1 LIMIT 1",
                        (run_id.as_str(),),
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Err(DbError::Row(format!("No run found with id '{run_id}'")));
                };
                row.text(0)
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    if !matches!(status.as_str(), "starting" | "live") {
        return Err(format!(
            "run {run_id} is {status}; only a live run's MCP attachment can be exported"
        ));
    }
    let request = cairn_common::protocol::CallbackRequest {
        run_id: Some(run_id.to_string()),
        ..Default::default()
    };
    let node_uri =
        crate::mcp::handlers::run_context::lookup_home_uri_routed(&orch.db, &request).await?;

    let (attachment, bearer) = attachment_for_run(orch, run_id, &node_uri)?;
    let (_, token) = mcp_credential_env(&attachment, &bearer)?;
    let McpTransport::Http { url, .. } = &attachment.transport else {
        unreachable!("attachment_for_run always builds an HTTP attachment");
    };
    let written = write_attachment_files(out_dir, &attachment, &token)?;
    log::info!(
        "MCP bearer {} for run {} exported to {}",
        bearer.lease_id(),
        run_id,
        out_dir.display()
    );
    Ok(DevAttachmentExport {
        run_id: run_id.to_string(),
        lease_id: bearer.lease_id().to_string(),
        url: url.clone(),
        token_env: written.0,
        claude_mcp_config: written.1,
        codex_config_override: written.2,
    })
}

#[cfg(debug_assertions)]
fn write_attachment_files(
    out_dir: &std::path::Path,
    attachment: &McpServer,
    token: &str,
) -> Result<(std::path::PathBuf, std::path::PathBuf, std::path::PathBuf), String> {
    use std::io::Write as _;
    #[cfg(unix)]
    use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};

    // Only a directory this call creates is owner-only by construction. An
    // existing one, the files already in it, or a symlink planted in its place
    // carry permissions and targets that are not ours to vouch for, so the
    // export directory must be new and each file is created exclusively
    // (`create_new` is O_EXCL, which also refuses to follow a symlink).
    if let Some(parent) = out_dir.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("cannot create {}: {error}", parent.display()))?;
    }
    let mut dir = std::fs::DirBuilder::new();
    #[cfg(unix)]
    dir.mode(0o700);
    dir.create(out_dir).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            format!(
                "{} already exists; export into a new directory so its owner-only permissions hold",
                out_dir.display()
            )
        } else {
            format!("cannot create {}: {error}", out_dir.display())
        }
    })?;

    let write = |name: &str, contents: String| -> Result<std::path::PathBuf, String> {
        let path = out_dir.join(name);
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options
            .open(&path)
            .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
        file.write_all(contents.as_bytes())
            .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
        Ok(path)
    };
    let servers = crate::backends::cairn_servers(attachment);
    Ok((
        write("token.env", format!("{MCP_TOKEN_ENV}={token}\n"))?,
        write("claude-mcp.json", servers.claude_mcp_config())?,
        write("codex.override", servers.codex_config_overrides().concat())?,
    ))
}

fn strip_port(host: &str) -> &str {
    if let Some(rest) = host.strip_prefix('[') {
        // `[::1]:3849` — the address is everything inside the brackets.
        return rest.split(']').next().unwrap_or(rest);
    }
    match host.rsplit_once(':') {
        // Exactly one colon followed by digits is a port; more colons is a bare
        // IPv6 address, which has no port without brackets.
        Some((name, port))
            if !name.contains(':')
                && !port.is_empty()
                && port.bytes().all(|b| b.is_ascii_digit()) =>
        {
            name
        }
        _ => host,
    }
}

fn digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

pub(crate) fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (l, r)| difference | (l ^ r))
        == 0
}

fn format_time(at: i64) -> String {
    chrono::DateTime::from_timestamp(at, 0)
        .map(|time| time.to_rfc3339())
        .unwrap_or_else(|| at.to_string())
}

/// What a bearer speaks for.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BearerScope {
    /// Exactly one run.
    Run(String),
    /// The calls on one pooled Codex server, by server id.
    CodexPool(String),
}

impl BearerScope {
    /// The run, for a per-run bearer.
    pub fn run_id(&self) -> Option<&str> {
        match self {
            Self::Run(run_id) => Some(run_id),
            Self::CodexPool(_) => None,
        }
    }

    /// What the scope is called in logs and refusals. Never a value.
    pub fn label(&self) -> String {
        match self {
            Self::Run(run_id) => format!("run {run_id}"),
            Self::CodexPool(server_id) => format!("pooled Codex server {server_id}"),
        }
    }
}

/// A minted bearer: what it speaks for and the lease holding its value.
///
/// Deliberately without `Debug`: the only way to its value is
/// [`Self::present`], which names where the value is going.
#[derive(Clone)]
pub struct RunBearer {
    scope: BearerScope,
    lease: CredentialLease,
}

/// Names the scope and the lease, never the value.
impl std::fmt::Debug for RunBearer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunBearer")
            .field("scope", &self.scope)
            .field("lease", self.lease.id())
            .finish()
    }
}

impl RunBearer {
    pub fn scope(&self) -> &BearerScope {
        &self.scope
    }

    pub fn lease_id(&self) -> &LeaseId {
        self.lease.id()
    }

    /// The destination this bearer was minted for.
    pub fn audience(&self) -> &LeaseAudience {
        self.lease.audience()
    }

    /// The token, for the places it is handed out: the agent or project service
    /// process that will present it to `audience` ([`mcp_credential_env`],
    /// [`service_credential`]), or a dev export.
    pub fn present(&self, audience: &LeaseAudience) -> Result<Presented<'_>, LeaseDenied> {
        self.lease.present(audience)
    }
}

/// A request's bearer, verified: what the request speaks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedBearer {
    pub scope: BearerScope,
    pub lease_id: LeaseId,
}

impl VerifiedBearer {
    /// The run, for a per-run bearer.
    pub fn run_id(&self) -> Option<&str> {
        self.scope.run_id()
    }
}

/// Why the endpoint refused a request's bearer.
///
/// Every message names runs, places, and times, never a value, because these
/// reach logs and the agent's MCP client.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BearerRefusal {
    #[error(
        "no bearer token: Cairn's MCP endpoint serves a run that presents its bearer in an \
         `Authorization: Bearer` header"
    )]
    Missing,
    #[error("the Authorization header is not a `Bearer <token>` credential")]
    Malformed,
    #[error(
        "this bearer was not issued by this Cairn orchestrator: it belongs to a run on another \
         host, or to one from before this orchestrator restarted. Tool calls are served only \
         by the process that spawned the run"
    )]
    Unknown,
    #[error("{holder} {reason}{}; its bearer is revoked", at.map(|at| format!(" at {}", format_time(at))).unwrap_or_default())]
    Revoked {
        /// What the bearer spoke for ([`BearerScope::label`]).
        holder: String,
        at: Option<i64>,
        reason: String,
    },
    #[error("this bearer is bound to {bound} and was presented to {presented}")]
    Audience { bound: String, presented: String },
    #[error("the bearer for {holder} expired at {}", format_time(*at))]
    Expired { holder: String, at: i64 },
    #[error("this bearer speaks for {holder}, not one run, and this endpoint serves runs alone")]
    NotARun { holder: String },
}

struct Entry {
    scope: BearerScope,
    lease: CredentialLease,
    revoked: Option<Revocation>,
}

#[derive(Clone)]
struct Revocation {
    at: i64,
    reason: String,
}

#[derive(Default)]
struct BookState {
    /// Bearers (live and tombstoned), keyed by token digest.
    entries: HashMap<[u8; 32], Entry>,
    /// Runs whose bearers have been revoked, including runs that had none yet.
    /// Minting consults this under the same lock revocation takes, so a mint
    /// racing finalization either lands first and is revoked with the rest, or
    /// lands after and is refused.
    revoked_runs: HashMap<String, Revocation>,
}

/// This orchestrator's run bearers.
#[derive(Default)]
pub struct RunBearerBook {
    state: Mutex<BookState>,
}

impl RunBearerBook {
    /// Mint a bearer for `run_id`, presentable only to `audience`. Refused once
    /// the run's bearers have been revoked.
    ///
    /// `node_uri` is the run's home node. It is recorded as the lease's scope,
    /// so `cairn://grants?view=leases` says which node a live bearer acts as.
    pub fn mint(
        &self,
        run_id: &str,
        node_uri: &str,
        audience: LeaseAudience,
    ) -> Result<RunBearer, BearerRefusal> {
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        let token = URL_SAFE_NO_PAD.encode(bytes);
        self.mint_token(
            run_id,
            node_uri,
            audience,
            now() + BEARER_LIFETIME_SECS,
            token,
        )
    }

    /// [`Self::mint`] with a shorter life, for a bearer whose run is bounded:
    /// a project service's per-call credential lives only as long as its call
    /// may, even if the revocation at the call's end never runs.
    pub fn mint_expiring(
        &self,
        run_id: &str,
        node_uri: &str,
        audience: LeaseAudience,
        lifetime: std::time::Duration,
    ) -> Result<RunBearer, BearerRefusal> {
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        let token = URL_SAFE_NO_PAD.encode(bytes);
        let lifetime = (lifetime.as_secs() as i64).clamp(1, BEARER_LIFETIME_SECS);
        self.mint_token(run_id, node_uri, audience, now() + lifetime, token)
    }

    /// Mint the bearer for pooled Codex server `server_id`, presentable only to
    /// `audience`. Ended by [`Self::revoke_pool`] when the server closes.
    pub fn mint_pool(&self, server_id: &str, audience: LeaseAudience) -> RunBearer {
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        let token = URL_SAFE_NO_PAD.encode(bytes);
        let source = CredentialSource::CodexPoolMcpBearer {
            server_id: server_id.to_string(),
            serial: BEARER_SERIAL.fetch_add(1, Ordering::Relaxed),
        };
        let lease = leases().issue(
            LeaseTerms {
                source: &source,
                audience,
                expires_at: now() + BEARER_LIFETIME_SECS,
                purpose: "pooled Codex MCP bearer",
            },
            token.clone(),
        );
        let scope = BearerScope::CodexPool(server_id.to_string());
        log::info!(
            "MCP bearer {} minted for {} ({})",
            lease.id(),
            scope.label(),
            lease.audience().label()
        );
        let mut state = self.state.lock().expect("run bearer book poisoned");
        state.entries.insert(
            digest(&token),
            Entry {
                scope: scope.clone(),
                lease: lease.clone(),
                revoked: None,
            },
        );
        RunBearer { scope, lease }
    }

    fn mint_token(
        &self,
        run_id: &str,
        node_uri: &str,
        audience: LeaseAudience,
        expires_at: i64,
        token: String,
    ) -> Result<RunBearer, BearerRefusal> {
        let at = now();
        // Held from the revocation check through the insert, so revocation
        // cannot slip between them.
        let mut state = self.state.lock().expect("run bearer book poisoned");
        if let Some(revocation) = state.revoked_runs.get(run_id) {
            return Err(BearerRefusal::Revoked {
                holder: format!("run {run_id}"),
                at: Some(revocation.at),
                reason: revocation.reason.clone(),
            });
        }
        let token_digest = digest(&token);
        let source = CredentialSource::RunMcpBearer {
            run_id: run_id.to_string(),
            node_uri: node_uri.to_string(),
            serial: BEARER_SERIAL.fetch_add(1, Ordering::Relaxed),
        };
        // Issuing registers the value for scrubbing before the lease exists.
        let lease = leases().issue(
            LeaseTerms {
                source: &source,
                audience,
                expires_at,
                purpose: "agent MCP bearer",
            },
            token,
        );
        log::info!(
            "MCP bearer {} minted for run {} ({})",
            lease.id(),
            run_id,
            lease.audience().label()
        );

        state.entries.retain(|_, entry| match &entry.revoked {
            Some(revocation) => revocation.at > at - TOMBSTONE_RETENTION_SECS,
            None => entry.lease.expires_at() > at - TOMBSTONE_RETENTION_SECS,
        });
        state
            .revoked_runs
            .retain(|_, revocation| revocation.at > at - TOMBSTONE_RETENTION_SECS);
        state.entries.insert(
            token_digest,
            Entry {
                scope: BearerScope::Run(run_id.to_string()),
                lease: lease.clone(),
                revoked: None,
            },
        );
        Ok(RunBearer {
            scope: BearerScope::Run(run_id.to_string()),
            lease,
        })
    }

    /// Whether `run_id`'s bearers have been revoked.
    pub fn run_is_revoked(&self, run_id: &str) -> bool {
        self.state
            .lock()
            .expect("run bearer book poisoned")
            .revoked_runs
            .contains_key(run_id)
    }

    /// Verify a presented token for a request that arrived at `presented_to`.
    pub fn verify(
        &self,
        token: &str,
        presented_to: &LeaseAudience,
    ) -> Result<VerifiedBearer, BearerRefusal> {
        let state = self.state.lock().expect("run bearer book poisoned");
        let entry = state
            .entries
            .get(&digest(token))
            .ok_or(BearerRefusal::Unknown)?;
        if let Some(revocation) = &entry.revoked {
            return Err(BearerRefusal::Revoked {
                holder: entry.scope.label(),
                at: Some(revocation.at),
                reason: revocation.reason.clone(),
            });
        }
        let verdict = match entry.lease.present(presented_to) {
            Ok(presented) if constant_time_eq(presented.expose().as_bytes(), token.as_bytes()) => {
                Ok(VerifiedBearer {
                    scope: entry.scope.clone(),
                    lease_id: entry.lease.id().clone(),
                })
            }
            Ok(_) => Err(BearerRefusal::Unknown),
            Err(LeaseDenied::Audience {
                bound, presented, ..
            }) => Err(BearerRefusal::Audience { bound, presented }),
            Err(LeaseDenied::Expired { expired_at, .. }) => Err(BearerRefusal::Expired {
                holder: entry.scope.label(),
                at: expired_at,
            }),
            // Revoked through the lease book rather than by finalization, as a
            // disclosure response does.
            Err(LeaseDenied::Revoked { .. }) => Err(BearerRefusal::Revoked {
                holder: entry.scope.label(),
                at: None,
                reason: "had its credential revoked".to_string(),
            }),
        };
        verdict
    }

    /// Revoke every bearer for `run_id`, leaving tombstones that name `reason`,
    /// and refuse any later mint for it. Idempotent. Returns how many bearers
    /// this call revoked.
    pub fn revoke_run(&self, run_id: &str, reason: &str) -> usize {
        let at = now();
        let mut state = self.state.lock().expect("run bearer book poisoned");
        state
            .revoked_runs
            .entry(run_id.to_string())
            .or_insert_with(|| Revocation {
                at,
                reason: reason.to_string(),
            });
        Self::revoke_scope(
            &mut state,
            &BearerScope::Run(run_id.to_string()),
            at,
            reason,
        )
    }

    /// Revoke pooled Codex server `server_id`'s bearers, leaving tombstones
    /// that name `reason`. Idempotent. Returns how many this call revoked.
    pub fn revoke_pool(&self, server_id: &str, reason: &str) -> usize {
        let mut state = self.state.lock().expect("run bearer book poisoned");
        Self::revoke_scope(
            &mut state,
            &BearerScope::CodexPool(server_id.to_string()),
            now(),
            reason,
        )
    }

    fn revoke_scope(state: &mut BookState, scope: &BearerScope, at: i64, reason: &str) -> usize {
        let mut revoked = 0;
        for entry in state.entries.values_mut() {
            if &entry.scope != scope || entry.revoked.is_some() {
                continue;
            }
            entry.lease.revoke();
            entry.revoked = Some(Revocation {
                at,
                reason: reason.to_string(),
            });
            log::info!(
                "MCP bearer {} for {} revoked: {}",
                entry.lease.id(),
                scope.label(),
                reason
            );
            revoked += 1;
        }
        revoked
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::Sanitizer;

    const NODE: &str = "cairn://p/CAIRN/4804/2/builder";

    fn token_of(bearer: &RunBearer) -> String {
        let Ok(presented) = bearer.present(bearer.audience()) else {
            panic!("a fresh bearer is presentable to its own audience");
        };
        presented.expose().to_string()
    }

    fn local() -> LeaseAudience {
        audience_for_host("127.0.0.1:3849")
    }

    #[test]
    fn a_service_credential_verifies_at_its_callback_and_is_refused_elsewhere() {
        let book = RunBearerBook::default();
        let callback = "http://127.0.0.1:3849/api/mcp";
        let bearer = book
            .mint("run-s", NODE, audience_for_url(callback).unwrap())
            .unwrap();
        let token = service_credential(&bearer, &local()).unwrap();
        assert_eq!(
            book.verify(&token, &local()).unwrap().run_id(),
            Some("run-s")
        );
        assert!(service_credential(&bearer, &audience_for_host("elsewhere.example")).is_err());

        book.revoke_run("run-s", "call ended");
        assert!(
            service_credential(&bearer, &local()).is_err(),
            "a revoked bearer must not be handed to a service"
        );
    }

    /// A remote service's bearers are bound to its executor's relay. They are
    /// refused at this runner's loopback, where the HTTP callback verifies, and
    /// at any other executor's relay; a loopback bearer is refused at a relay.
    #[test]
    fn a_relay_bearer_and_a_loopback_bearer_are_each_refused_at_the_other() {
        let book = RunBearerBook::default();
        let relayed = book
            .mint("run-r", NODE, relay_audience("machine-b"))
            .unwrap();
        let token = service_credential(&relayed, &relay_audience("machine-b")).unwrap();
        assert_eq!(
            book.verify(&token, &relay_audience("machine-b"))
                .unwrap()
                .run_id(),
            Some("run-r")
        );
        assert!(matches!(
            book.verify(&token, &local()),
            Err(BearerRefusal::Audience { .. })
        ));
        assert!(matches!(
            book.verify(&token, &relay_audience("machine-c")),
            Err(BearerRefusal::Audience { .. })
        ));
        assert!(service_credential(&relayed, &local()).is_err());

        let looped = book.mint("run-l", NODE, local()).unwrap();
        let token = service_credential(&looped, &local()).unwrap();
        assert!(matches!(
            book.verify(&token, &relay_audience("machine-b")),
            Err(BearerRefusal::Audience { .. })
        ));
    }

    #[test]
    fn a_bound_request_names_its_run_alone() {
        let mut request = crate::mcp::types::McpCallbackRequest::default();
        bind_request_to_run(&mut request, "run-a").unwrap();
        assert_eq!(request.run_id.as_deref(), Some("run-a"));
        let mut claimed = crate::mcp::types::McpCallbackRequest {
            run_id: Some("run-b".into()),
            ..Default::default()
        };
        assert!(bind_request_to_run(&mut claimed, "run-a").is_err());
        // Codex stamps a thread on every call: the run stays the credential's own,
        // with nothing left for dispatch to re-attribute.
        let mut threaded = crate::mcp::types::McpCallbackRequest {
            thread_id: Some("t".into()),
            ..Default::default()
        };
        bind_request_to_run(&mut threaded, "run-a").unwrap();
        assert_eq!(threaded.run_id.as_deref(), Some("run-a"));
        assert_eq!(threaded.thread_id, None);
    }

    #[test]
    fn a_minted_bearer_verifies_as_its_run() {
        let book = RunBearerBook::default();
        let bearer = book.mint("run-a", NODE, local()).unwrap();
        let verified = book.verify(&token_of(&bearer), &local()).unwrap();
        assert_eq!(verified.run_id(), Some("run-a"));
        assert_eq!(&verified.lease_id, bearer.lease_id());
    }

    #[test]
    fn each_bearer_names_only_its_own_run() {
        let book = RunBearerBook::default();
        let a = book.mint("run-a", NODE, local()).unwrap();
        let b = book.mint("run-b", NODE, local()).unwrap();
        assert_eq!(
            book.verify(&token_of(&a), &local()).unwrap().run_id(),
            Some("run-a")
        );
        assert_eq!(
            book.verify(&token_of(&b), &local()).unwrap().run_id(),
            Some("run-b")
        );
    }

    #[test]
    fn a_revoked_bearer_is_refused_naming_its_run_and_why() {
        let book = RunBearerBook::default();
        let bearer = book.mint("run-a", NODE, local()).unwrap();
        let token = token_of(&bearer);
        let second = book.mint("run-a", NODE, local()).unwrap();
        let other = book.mint("run-b", NODE, local()).unwrap();

        assert_eq!(book.revoke_run("run-a", "finalized (exited)"), 2);
        assert_eq!(book.revoke_run("run-a", "finalized (exited)"), 0);

        let refusal = book.verify(&token, &local()).unwrap_err();
        let BearerRefusal::Revoked { holder, at, reason } = &refusal else {
            panic!("expected a revoked refusal, got {refusal:?}");
        };
        assert_eq!(holder, "run run-a");
        assert!(at.is_some());
        assert_eq!(reason, "finalized (exited)");
        let message = refusal.to_string();
        assert!(
            message.contains("run run-a finalized (exited) at "),
            "{message}"
        );
        assert!(!message.contains(&token));
        assert!(second.present(second.audience()).is_err());
        assert!(book.verify(&token_of(&other), &local()).is_ok());
    }

    /// Finalization can win the race with a mint that already checked the run
    /// was live (the export path does exactly that check, then awaits a lookup,
    /// then mints). The mint must lose, whether or not the run had any bearer
    /// when it was revoked.
    #[test]
    fn a_run_revoked_before_a_mint_refuses_it() {
        let book = RunBearerBook::default();
        assert_eq!(book.revoke_run("run-empty", "finalized (exited)"), 0);
        assert!(book.run_is_revoked("run-empty"));
        let refusal = book.mint("run-empty", NODE, local()).unwrap_err();
        assert!(
            matches!(&refusal, BearerRefusal::Revoked { holder, reason, .. }
                if holder == "run run-empty" && reason == "finalized (exited)"),
            "{refusal:?}"
        );

        let first = book.mint("run-busy", NODE, local()).unwrap();
        let token = token_of(&first);
        book.revoke_run("run-busy", "finalized (crashed)");
        assert!(book.mint("run-busy", NODE, local()).is_err());
        assert!(matches!(
            book.verify(&token, &local()),
            Err(BearerRefusal::Revoked { .. })
        ));
        assert!(!book.run_is_revoked("run-other"));
        assert!(book.mint("run-other", NODE, local()).is_ok());
    }

    #[test]
    fn a_bearer_this_book_never_minted_is_unknown() {
        let predecessor = RunBearerBook::default();
        let successor = RunBearerBook::default();
        let bearer = predecessor.mint("run-a", NODE, local()).unwrap();
        let refusal = successor.verify(&token_of(&bearer), &local()).unwrap_err();
        assert_eq!(refusal, BearerRefusal::Unknown);
        assert!(refusal
            .to_string()
            .contains("only by the process that spawned the run"));
        assert_eq!(
            successor.verify("not-a-token", &local()).unwrap_err(),
            BearerRefusal::Unknown
        );
    }

    #[test]
    fn a_bearer_is_refused_at_a_host_it_was_not_minted_for() {
        let book = RunBearerBook::default();
        let public = audience_for_url("https://orch.tail1234.ts.net/mcp").unwrap();
        let bearer = book.mint("run-a", NODE, public.clone()).unwrap();
        let token = token_of(&bearer);

        assert!(book
            .verify(&token, &audience_for_host("orch.tail1234.ts.net"))
            .is_ok());
        for elsewhere in ["127.0.0.1:3849", "evil.example.com"] {
            let refusal = book
                .verify(&token, &audience_for_host(elsewhere))
                .unwrap_err();
            assert!(
                matches!(refusal, BearerRefusal::Audience { .. }),
                "{elsewhere}: {refusal:?}"
            );
        }
    }

    #[test]
    fn an_expired_bearer_is_refused() {
        let book = RunBearerBook::default();
        let token = "expired-bearer-Wq84Zm19Kd62Lp37".to_string();
        book.mint_token("run-a", NODE, local(), now() - 1, token.clone())
            .unwrap();
        let refusal = book.verify(&token, &local()).unwrap_err();
        assert!(
            matches!(&refusal, BearerRefusal::Expired { holder, .. } if holder == "run run-a"),
            "{refusal:?}"
        );
    }

    /// A pool bearer verifies as its pooled server, never as a run; revoking
    /// the pool ends it and leaves run bearers alone, and finalizing a run
    /// does not end the pool's.
    #[test]
    fn a_pool_bearer_speaks_for_its_server_and_dies_with_it() {
        let book = RunBearerBook::default();
        let pool = book.mint_pool("codex-pool-7", local());
        let run = book.mint("run-a", NODE, local()).unwrap();
        let token = token_of(&pool);

        let verified = book.verify(&token, &local()).unwrap();
        assert_eq!(
            verified.scope,
            BearerScope::CodexPool("codex-pool-7".into())
        );
        assert_eq!(verified.run_id(), None);

        book.revoke_run("codex-pool-7", "finalized (exited)");
        assert!(book.verify(&token, &local()).is_ok());

        assert_eq!(book.revoke_pool("codex-pool-7", "server closed"), 1);
        let refusal = book.verify(&token, &local()).unwrap_err();
        assert!(
            refusal
                .to_string()
                .starts_with("pooled Codex server codex-pool-7 server closed"),
            "{refusal}"
        );
        assert!(book.verify(&token_of(&run), &local()).is_ok());
    }

    #[test]
    fn both_attachments_carry_a_bearer_under_their_own_variable() {
        let book = RunBearerBook::default();
        let stdio = crate::config::mcp_setup::stdio_attachment(
            "/usr/bin/cairn-cmd",
            3849,
            None,
            "dev",
            None,
            "info",
        );
        let bearer = book
            .mint("run-a", NODE, attachment_audience(&stdio).unwrap())
            .unwrap();
        let (var, value) = mcp_credential_env(&stdio, &bearer).unwrap();
        assert_eq!(var, STDIO_CREDENTIAL_ENV);
        assert_eq!(
            book.verify(&value, &local()).unwrap().run_id(),
            Some("run-a")
        );

        let http = McpServer::cairn_http("https://orch.tail1234.ts.net/mcp");
        assert!(
            mcp_credential_env(&http, &bearer).is_err(),
            "a loopback bearer is not presentable to a public endpoint"
        );
    }

    #[test]
    fn the_run_less_allowlist_is_pinned() {
        assert_eq!(
            RUN_LESS_TOOLS,
            [
                "read_batch",
                "read_resource",
                "read_issue_resource",
                "watch",
                "process_info",
            ]
        );
    }

    fn machine_request(tool: &str) -> crate::mcp::types::McpCallbackRequest {
        crate::mcp::types::McpCallbackRequest {
            tool: tool.into(),
            ..Default::default()
        }
    }

    #[test]
    fn the_machine_credential_claims_no_run_and_reaches_only_its_tools() {
        for tool in RUN_LESS_TOOLS {
            let mut request = machine_request(tool);
            hold_to_machine_credential(&mut request).unwrap();
            assert!(
                request.is_unverified_local(),
                "an admitted request carries the mark that labels and limits it"
            );
        }
        for tool in ["write", "run", "check_run", "read", "read_web", "anything"] {
            let refusal = hold_to_machine_credential(&mut machine_request(tool)).unwrap_err();
            assert!(refusal.contains(tool), "{refusal}");
        }

        let mut claiming = machine_request("read_batch");
        claiming.run_id = Some("run-a".into());
        let refusal = hold_to_machine_credential(&mut claiming).unwrap_err();
        assert!(refusal.contains("claims run run-a"), "{refusal}");

        let mut threaded = machine_request("read_batch");
        threaded.thread_id = Some("thread-a".into());
        hold_to_machine_credential(&mut threaded).unwrap();
        assert_eq!(
            threaded.thread_id, None,
            "a run-less caller borrows no thread"
        );

        // An empty claim is how a run-less client serializes "no run".
        let mut empty = machine_request("read_batch");
        empty.run_id = Some(String::new());
        hold_to_machine_credential(&mut empty).unwrap();
        assert_eq!(empty.run_id, None);
    }

    fn write_request(
        target: &str,
        payload: serde_json::Value,
    ) -> crate::mcp::types::McpCallbackRequest {
        let mut request = machine_request("write");
        request.payload = serde_json::json!({
            "changes": [{"target": target, "mode": "append", "payload": payload}]
        });
        request
    }

    /// The machine credential's writes: a self-rebuild delivery and a message to
    /// a `/messages` collection, with `content` only. Nothing else, including
    /// everything that shares a shape with those two.
    #[test]
    fn the_machine_credential_writes_only_a_delivery_or_a_message() {
        let content = || serde_json::json!({"content": "hello"});
        for target in [
            "cairn://self-rebuild/outcome-1",
            "cairn://p/cairn/5949/messages",
            "cairn://p/cairn/5949/1/builder/messages",
            "cairn://p/cairn/5949/1/builder/task/review/messages",
            "cairn://p/cairn/security/messages",
            "cairn://p/cairn/security/task/review/messages",
        ] {
            hold_to_machine_credential(&mut write_request(target, content()))
                .unwrap_or_else(|refusal| panic!("{target}: {refusal}"));
        }
        for target in [
            "cairn://self-rebuild",
            "cairn://p/cairn/5949",
            "cairn://p/cairn/5949/comments",
            "cairn://p/cairn/messages",
            "cairn://p/cairn/security",
            "cairn://p/cairn/security/db/messages",
            "cairn://p/cairn/5949/1/builder",
            "cairn://p/cairn/5949/1/builder/tasks",
            "cairn://p/cairn/5949/1/builder/questions",
            "cairn://grants",
            "cairn://settings",
            "cairn://dev/launch-ticket",
            "file:src/lib.rs",
        ] {
            assert!(
                hold_to_machine_credential(&mut write_request(target, content())).is_err(),
                "{target}"
            );
        }
        let messages = "cairn://p/cairn/5949/1/builder/messages";
        for payload in [
            serde_json::json!({"content": "hello", "escalate": true}),
            serde_json::json!({"content": 1}),
            serde_json::json!({}),
        ] {
            assert!(
                hold_to_machine_credential(&mut write_request(messages, payload.clone())).is_err(),
                "{payload}"
            );
        }
        let mut committing = write_request(messages, content());
        committing.payload["commit_msg"] = "nope".into();
        assert!(hold_to_machine_credential(&mut committing).is_err());
    }

    /// A resident process started for no run is held to the same list, plus the
    /// launch ticket `bun dev:instance` asks for from a project terminal.
    #[test]
    fn a_run_less_process_may_also_ask_for_a_launch_ticket() {
        let ticket = || write_request("cairn://dev/launch-ticket", serde_json::json!({}));
        hold_to_run_less_process(&mut ticket()).unwrap();
        assert!(hold_to_machine_credential(&mut ticket()).is_err());
        assert!(hold_to_run_less_process(&mut write_request(
            "cairn://p/cairn/5949",
            serde_json::json!({"content": "x"})
        ))
        .is_err());
        assert!(hold_to_run_less_process(&mut machine_request("run")).is_err());
    }

    /// A pool bearer naming no thread, or a thread no server of its id holds,
    /// is refused.
    #[test]
    fn a_pool_request_must_name_one_of_its_own_threads() {
        let pools = crate::backends::codex::pool::CodexAppServerPool::default();
        let book = RunBearerBook::default();
        let mut threadless = crate::mcp::types::McpCallbackRequest {
            run_id: Some(POOLED_CODEX_RUN_SENTINEL.into()),
            ..Default::default()
        };
        let refusal = bind_request_to_pool_thread(&pools, &book, &mut threadless, "codex-pool-x")
            .unwrap_err();
        assert!(refusal.contains("names none of its threads"), "{refusal}");
        let mut foreign = crate::mcp::types::McpCallbackRequest {
            thread_id: Some("thread-elsewhere".into()),
            ..Default::default()
        };
        let refusal =
            bind_request_to_pool_thread(&pools, &book, &mut foreign, "codex-pool-x").unwrap_err();
        assert!(refusal.contains("is not a live call"), "{refusal}");
    }

    /// A pool with one live server, which lives while the returned sender does.
    fn live_pooled_server() -> (
        std::sync::Arc<crate::backends::codex::pool::CodexAppServerPool>,
        std::sync::Arc<crate::backends::codex::pool::PooledAppServer>,
        crossbeam_channel::Sender<serde_json::Value>,
    ) {
        let pools =
            std::sync::Arc::new(crate::backends::codex::pool::CodexAppServerPool::default());
        let (server, stream) = crate::backends::codex::pool::testing::live_server(&pools);
        (pools, server, stream)
    }

    fn pool_thread_request(thread: &str) -> crate::mcp::types::McpCallbackRequest {
        crate::mcp::types::McpCallbackRequest {
            run_id: Some(POOLED_CODEX_RUN_SENTINEL.into()),
            thread_id: Some(thread.into()),
            ..Default::default()
        }
    }

    /// A live call on the server is admitted and attributed to its run, and a
    /// claim to any other run is refused.
    #[test]
    fn a_pool_request_is_attributed_to_its_thread_s_run() {
        let (pools, server, _stream) = live_pooled_server();
        let book = RunBearerBook::default();
        let _call = server.register_call("thread-a", "run-a");

        let mut own = pool_thread_request("thread-a");
        bind_request_to_pool_thread(&pools, &book, &mut own, server.id()).unwrap();
        assert_eq!(own.run_id.as_deref(), Some("run-a"));

        let mut forged = pool_thread_request("thread-a");
        forged.run_id = Some("run-b".into());
        let refusal =
            bind_request_to_pool_thread(&pools, &book, &mut forged, server.id()).unwrap_err();
        assert!(refusal.contains("claims run run-b"), "{refusal}");

        let mut elsewhere = pool_thread_request("thread-a");
        assert!(
            bind_request_to_pool_thread(&pools, &book, &mut elsewhere, "codex-pool-other").is_err(),
            "a bearer for another server speaks for none of this one's threads"
        );
    }

    /// The pool's bearer outlives every run it serves, so the run's end is what
    /// has to stop it. A thread whose run is revoked is refused even with its
    /// binding still in place, which is what a call registering during
    /// finalization leaves behind; the run's neighbour is unaffected.
    #[test]
    fn a_pool_request_for_a_finalized_run_is_refused() {
        let (pools, server, _stream) = live_pooled_server();
        let book = RunBearerBook::default();
        let _ended = server.register_call("thread-ended", "run-ended");
        let _other = server.register_call("thread-other", "run-other");

        book.revoke_run("run-ended", "finalized (exited)");

        let mut late = pool_thread_request("thread-ended");
        let refusal =
            bind_request_to_pool_thread(&pools, &book, &mut late, server.id()).unwrap_err();
        assert!(refusal.contains("has finalized"), "{refusal}");
        assert_eq!(
            late.run_id.as_deref(),
            Some(POOLED_CODEX_RUN_SENTINEL),
            "a refused request attributes nothing"
        );

        let mut neighbour = pool_thread_request("thread-other");
        bind_request_to_pool_thread(&pools, &book, &mut neighbour, server.id()).unwrap();
        assert_eq!(neighbour.run_id.as_deref(), Some("run-other"));
    }

    #[test]
    fn bearers_appear_in_the_lease_inventory_until_revoked() {
        let book = RunBearerBook::default();
        let bearer = book.mint("run-inventory", NODE, local()).unwrap();
        let row = |id: &LeaseId| {
            leases()
                .inventory()
                .into_iter()
                .find(|record| &record.id == id)
                .expect("the bearer's lease is in the inventory")
        };
        let active = row(bearer.lease_id());
        assert_eq!(active.status, "active");
        assert!(active.scope.contains("4804"), "{}", active.scope);

        book.revoke_run("run-inventory", "finalized (exited)");
        assert!(matches!(
            bearer.present(bearer.audience()),
            Err(LeaseDenied::Revoked { .. })
        ));
        // The process-wide book sweeps dead leases whenever any lease is issued,
        // which a concurrently running test can do at any moment, so a revoked
        // lease is listed as revoked or already gone. Never as active.
        let after = leases()
            .inventory()
            .into_iter()
            .find(|record| &record.id == bearer.lease_id());
        assert!(
            after
                .as_ref()
                .is_none_or(|record| record.status == "revoked"),
            "{:?}",
            after.map(|record| record.status)
        );
    }

    #[test]
    fn a_minted_token_is_scrubbed_and_never_debug_printed() {
        let book = RunBearerBook::default();
        let bearer = book.mint("run-a", NODE, local()).unwrap();
        let token = token_of(&bearer);
        let mut sanitizer = Sanitizer::exact();
        let header = format!("Authorization: Bearer {token}");
        let scrubbed = sanitizer.text(&header);
        assert!(!scrubbed.contains(&token), "{scrubbed}");

        let verified = book.verify(&token, &local()).unwrap();
        assert!(!format!("{verified:?}").contains(&token));
    }

    #[cfg(unix)]
    #[test]
    fn exported_files_are_owner_only_and_only_token_env_holds_the_token() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("export");
        let attachment = McpServer::cairn_http("https://orch.tail1234.ts.net/mcp");
        let token = "exported-bearer-Qm47Zx82Lp19Wd";
        let (token_env, claude, codex) = write_attachment_files(&out, &attachment, token).unwrap();

        let mode =
            |path: &std::path::Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&out), 0o700);
        for path in [&token_env, &claude, &codex] {
            assert_eq!(mode(path), 0o600, "{}", path.display());
        }
        assert_eq!(
            std::fs::read_to_string(&token_env).unwrap(),
            format!("CAIRN_MCP_TOKEN={token}\n")
        );
        for path in [&claude, &codex] {
            let text = std::fs::read_to_string(path).unwrap();
            assert!(!text.contains(token), "{}", path.display());
            assert!(text.contains("CAIRN_MCP_TOKEN"), "{}", path.display());
        }
    }

    /// An existing directory (with a world-readable `token.env` already in it)
    /// or a symlink in the directory's place is refused, and nothing is written
    /// through either.
    #[cfg(unix)]
    #[test]
    fn export_refuses_an_existing_directory_or_a_symlink() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let attachment = McpServer::cairn_http("https://orch.tail1234.ts.net/mcp");
        let token = "exported-bearer-Zk38Wq71Lm46Pd";

        let existing = dir.path().join("existing");
        std::fs::create_dir(&existing).unwrap();
        std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(existing.join("token.env"), "stale\n").unwrap();
        std::fs::set_permissions(
            existing.join("token.env"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        let error = write_attachment_files(&existing, &attachment, token).unwrap_err();
        assert!(error.contains("already exists"), "{error}");
        assert_eq!(
            std::fs::read_to_string(existing.join("token.env")).unwrap(),
            "stale\n"
        );

        let target = dir.path().join("target");
        std::fs::create_dir(&target).unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(write_attachment_files(&link, &attachment, token).is_err());
        assert!(std::fs::read_dir(&target).unwrap().next().is_none());
    }

    #[test]
    fn audiences_derive_from_hosts_with_or_without_ports() {
        let loopback = LeaseAudience::process(AGENT_MCP_ROLE);
        for host in [
            "127.0.0.1",
            "127.0.0.1:3849",
            "localhost:3849",
            "[::1]:3849",
            "::1",
        ] {
            assert_eq!(audience_for_host(host), loopback, "{host}");
        }
        assert_eq!(
            audience_for_host("Orch.Tail1234.ts.net:443"),
            LeaseAudience::https("orch.tail1234.ts.net")
        );
        assert_eq!(
            audience_for_url("http://127.0.0.1:3849/mcp").unwrap(),
            loopback
        );
        assert!(audience_for_url("not a url").is_err());
    }
}
