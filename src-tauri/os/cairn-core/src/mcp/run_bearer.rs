//! Per-run bearer tokens for the orchestrator's streamable-HTTP MCP endpoint.
//!
//! # Why a bearer per run
//!
//! The stdio path authenticates with the machine's MCP secret: one value, stable
//! across restarts, held by every agent this machine spawns. That is tolerable
//! only because the agent runs on the same machine as the runner and its run id
//! arrives in the environment the runner gave it. An agent on another host
//! presents nothing but what its MCP client sends, so the credential itself has
//! to say which run is calling. A run bearer does: it identifies exactly one
//! run, the endpoint attributes every call to that run from the token alone,
//! and it stops working when the run finalizes.
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
//! lives only inside its lease, from which it is presented once into the child
//! environment (or a dev export) and compared in constant time on each request.
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

use cairn_llm::session::mcp::{McpServer, McpTransport, MCP_TOKEN_ENV};

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
///
/// The one way a run gets an HTTP attachment: session start uses it today, and
/// a hub's session-create request will carry what it returns.
pub fn attachment_for_run(
    orch: &Orchestrator,
    run_id: &str,
    node_uri: &str,
) -> Result<(McpServer, RunBearer), String> {
    let url = public_mcp_url(orch);
    let audience = audience_for_url(&url)?;
    let bearer = orch
        .run_bearers
        .mint(run_id, node_uri, audience)
        .map_err(|refusal| refusal.to_string())?;
    Ok((McpServer::cairn_http(url), bearer))
}

/// The environment variable, and its value, that an agent process's MCP entry
/// authenticates with: the machine secret for the stdio shim, or the run's
/// bearer for the HTTP endpoint.
pub fn mcp_credential_env(
    orch: &Orchestrator,
    attachment: &McpServer,
    bearer: Option<&RunBearer>,
) -> Result<(&'static str, String), String> {
    match &attachment.transport {
        McpTransport::Stdio { .. } => orch
            .mcp_auth
            .get_secret_for_mcp()
            .map(|secret| ("CAIRN_MCP_SECRET", secret))
            .map_err(|e| format!("Failed to get MCP auth secret: {e}")),
        McpTransport::Http { url, .. } => {
            let bearer = bearer.ok_or_else(|| {
                "an HTTP MCP attachment needs its run's bearer, and none was minted".to_string()
            })?;
            // Presented to the agent that will carry it to `url`, so the
            // audience is derived from where it is going.
            let presented = bearer
                .present(&audience_for_url(url)?)
                .map_err(|denied| format!("run bearer not presentable: {denied}"))?;
            Ok((MCP_TOKEN_ENV, presented.expose().to_string()))
        }
    }
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
    let (_, token) = mcp_credential_env(orch, &attachment, Some(&bearer))?;
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

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
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

/// A minted bearer: the run it speaks for and the lease holding its value.
///
/// Deliberately without `Debug`: the only way to its value is
/// [`Self::present`], which names where the value is going.
#[derive(Clone)]
pub struct RunBearer {
    run_id: String,
    lease: CredentialLease,
}

/// Names the run and the lease, never the value.
impl std::fmt::Debug for RunBearer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunBearer")
            .field("run_id", &self.run_id)
            .field("lease", self.lease.id())
            .finish()
    }
}

impl RunBearer {
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn lease_id(&self) -> &LeaseId {
        self.lease.id()
    }

    /// The destination this bearer was minted for.
    pub fn audience(&self) -> &LeaseAudience {
        self.lease.audience()
    }

    /// The token, for the one place it is handed out: the agent process's
    /// environment, or a dev export, for an agent that will present it to
    /// `audience`.
    pub fn present(&self, audience: &LeaseAudience) -> Result<Presented<'_>, LeaseDenied> {
        self.lease.present(audience)
    }
}

/// A request's bearer, verified: the run the request speaks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedBearer {
    pub run_id: String,
    pub lease_id: LeaseId,
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
    #[error("run {run_id} {reason}{}; its bearer is revoked", at.map(|at| format!(" at {}", format_time(at))).unwrap_or_default())]
    Revoked {
        run_id: String,
        at: Option<i64>,
        reason: String,
    },
    #[error("this bearer is bound to {bound} and was presented to {presented}")]
    Audience { bound: String, presented: String },
    #[error("the bearer for run {run_id} expired at {}", format_time(*at))]
    Expired { run_id: String, at: i64 },
}

struct Entry {
    run_id: String,
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
                run_id: run_id.to_string(),
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
                run_id: run_id.to_string(),
                lease: lease.clone(),
                revoked: None,
            },
        );
        Ok(RunBearer {
            run_id: run_id.to_string(),
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
                run_id: entry.run_id.clone(),
                at: Some(revocation.at),
                reason: revocation.reason.clone(),
            });
        }
        let verdict = match entry.lease.present(presented_to) {
            Ok(presented) if constant_time_eq(presented.expose().as_bytes(), token.as_bytes()) => {
                Ok(VerifiedBearer {
                    run_id: entry.run_id.clone(),
                    lease_id: entry.lease.id().clone(),
                })
            }
            Ok(_) => Err(BearerRefusal::Unknown),
            Err(LeaseDenied::Audience {
                bound, presented, ..
            }) => Err(BearerRefusal::Audience { bound, presented }),
            Err(LeaseDenied::Expired { expired_at, .. }) => Err(BearerRefusal::Expired {
                run_id: entry.run_id.clone(),
                at: expired_at,
            }),
            // Revoked through the lease book rather than by finalization, as a
            // disclosure response does.
            Err(LeaseDenied::Revoked { .. }) => Err(BearerRefusal::Revoked {
                run_id: entry.run_id.clone(),
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
        let mut revoked = 0;
        for entry in state.entries.values_mut() {
            if entry.run_id != run_id || entry.revoked.is_some() {
                continue;
            }
            entry.lease.revoke();
            entry.revoked = Some(Revocation {
                at,
                reason: reason.to_string(),
            });
            log::info!(
                "MCP bearer {} for run {} revoked: {}",
                entry.lease.id(),
                run_id,
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
    fn a_minted_bearer_verifies_as_its_run() {
        let book = RunBearerBook::default();
        let bearer = book.mint("run-a", NODE, local()).unwrap();
        let verified = book.verify(&token_of(&bearer), &local()).unwrap();
        assert_eq!(verified.run_id, "run-a");
        assert_eq!(&verified.lease_id, bearer.lease_id());
    }

    #[test]
    fn each_bearer_names_only_its_own_run() {
        let book = RunBearerBook::default();
        let a = book.mint("run-a", NODE, local()).unwrap();
        let b = book.mint("run-b", NODE, local()).unwrap();
        assert_eq!(
            book.verify(&token_of(&a), &local()).unwrap().run_id,
            "run-a"
        );
        assert_eq!(
            book.verify(&token_of(&b), &local()).unwrap().run_id,
            "run-b"
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
        let BearerRefusal::Revoked { run_id, at, reason } = &refusal else {
            panic!("expected a revoked refusal, got {refusal:?}");
        };
        assert_eq!(run_id, "run-a");
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
            matches!(&refusal, BearerRefusal::Revoked { run_id, reason, .. }
                if run_id == "run-empty" && reason == "finalized (exited)"),
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
            matches!(&refusal, BearerRefusal::Expired { run_id, .. } if run_id == "run-a"),
            "{refusal:?}"
        );
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
