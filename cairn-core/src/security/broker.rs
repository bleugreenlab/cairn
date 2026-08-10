//! The credential broker: the one place a stored credential becomes plaintext.
//!
//! # Why a broker rather than an accessor
//!
//! Before this module, any code that wanted a configured MCP token called the
//! keychain accessor directly and got a `String`. A `String` is `Debug`, it is
//! `Serialize`, it is `Clone`, and nothing about it says "this is a credential".
//! That is how resolved credentials ended up expanded into a config struct that
//! was then serialized into a durable database row, and how an environment-
//! resolved token reached an external server without ever being registered for
//! scrubbing.
//!
//! Everything credential-shaped now goes through here, and the broker does three
//! things no accessor can:
//!
//! 1. **It registers before it returns.** A value is a scrub target from the
//!    instant it exists, so there is no window in which plaintext is live and
//!    unprotected.
//! 2. **It hands back a carrier, not a `String`.** [`BrokeredSecret`] and
//!    [`BrokeredMcpConfig`] have no `Debug`, no `Display`, no `serde`, and no
//!    `Clone`. A resolved credential cannot be logged or persisted by accident;
//!    reaching the plaintext takes a named call at the injection point.
//! 3. **It names the authority being exercised.** Every resolution mints a
//!    CAIRN-3803 [`AuthorityScope`] from the *resolved target*, never from
//!    caller-supplied text, and always at [`AuthorityAction::Run`].
//!
//! # Use is not reveal
//!
//! Credential use and credential reveal are distinct authorities, and here that
//! distinction is structural rather than documentary: every scope this module
//! mints carries [`AuthorityAction::Run`], and there is no entry point that
//! mints [`AuthorityAction::Read`]. "Read the value" is not a request the broker
//! knows how to answer, so no policy change or grant can turn one into a reveal.
//!
//! `purpose` is audit metadata. It is recorded with the resolution and is never
//! consulted to decide anything.
//!
//! # The declared-secret signal
//!
//! `${VAR}` interpolation in an MCP server config reaches `command`, `args`,
//! `url`, and `headers` as well as `env`, so the process-environment fallback
//! routinely resolves ordinary configuration — `args: ["${HOME}/Documents"]`
//! resolves a home directory, which clears the registry's length and variety
//! thresholds comfortably.
//!
//! Registering that would be worse than leaving it unscrubbed. A registered
//! value makes every model-authored write mentioning it *refused* at the
//! inbound crossing, behind a deliberately generic refusal the agent cannot
//! diagnose and will retry into. Over-redaction costs a mangled string;
//! over-registration costs the agent's ability to write.
//!
//! So the broker does not guess from the bytes. It asks whether the value was
//! *declared* to be a credential, and there are exactly two ways to declare one:
//!
//! - **By storage.** A value in the OS keychain is there because a human typed
//!   it into the settings UI's secret field. Choosing that store is the
//!   declaration.
//! - **By configuration.** A server's `secrets:` list names the `${VAR}`s that
//!   carry credentials. This is what brings an environment-resolved token —
//!   a developer's exported `LINEAR_API_KEY` — back under the registry, which a
//!   better length heuristic could never do.
//!
//! An undeclared environment value resolves and expands exactly as before, and
//! is not registered.

use std::collections::HashMap;

use cairn_common::authorization::{AuthorityAction, AuthorityPlace, AuthorityScope, ToolKind};
use zeroize::Zeroizing;

use crate::config::mcp_servers::McpServerConfig;

use super::registry::registry;
use super::secret::{SecretCategory, SecretId, SecretMaterial};

/// The workspace every brokered credential is scoped to.
///
/// Cairn is single-workspace today; the constant is the same one the
/// authorization normalizers use, so a brokered scope and an approved grant
/// speak about the same place.
fn workspace_id() -> String {
    crate::authorization::WORKSPACE_ID.to_string()
}

/// Whether a reference names a credential, and therefore whether the resolved
/// value becomes a scrub target. See the module docs on the declared-secret
/// signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Declared {
    /// A credential. Registered for scrubbing before it is returned.
    Secret,
    /// Ordinary configuration that happens to be interpolated — a path, a host,
    /// a flag. Resolved and expanded, never registered.
    Configuration,
}

/// Which store holds a credential, and under what key.
///
/// Non-secret by construction: every field names a location, never a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialSource {
    /// A `${VAR}` referenced by a configured MCP server, resolved from the
    /// keychain and then the process environment.
    McpVar {
        /// The scoped credential key (`linear`, `project/{path}/linear`).
        credential_key: String,
        var: String,
    },
    /// The OAuth access token for a configured MCP server.
    McpOAuth { credential_key: String },
    /// A web search or web fetch provider's API key.
    WebProvider { provider: String, var: String },
    /// The GitHub App's RSA private key. Cairn signs with it and never sends
    /// it: see [`github`].
    GitHubApp { app_id: i64 },
    /// A GitHub App installation access token — what the signature buys, and
    /// what actually travels to GitHub.
    GitHubInstallation { installation_id: i64 },
    /// A model backend's API key or OAuth token, by provider and account.
    ModelBackend {
        provider: String,
        account_id: String,
    },
    /// This desktop's own account credential for the Cairn API.
    AccountDevice,
    /// A team's sync token, minted against the device credential.
    TeamSync { team_id: String },
}

/// The provider name every GitHub credential is scoped under.
const GITHUB_PROVIDER: &str = "github";

/// The provider name Cairn's own cloud credentials are scoped under.
const CAIRN_CLOUD_PROVIDER: &str = "cairn-cloud";

impl CredentialSource {
    /// The place and action this resolution exercises.
    ///
    /// Derived from the resolved target rather than supplied by the caller: a
    /// scope a caller can name is a scope a caller can widen. The action is
    /// always [`AuthorityAction::Run`] — see the module docs on use versus
    /// reveal.
    pub fn scope(&self) -> AuthorityScope {
        let place = match self {
            Self::McpVar { credential_key, .. } | Self::McpOAuth { credential_key } => {
                AuthorityPlace::Tool {
                    workspace_id: workspace_id(),
                    kind: ToolKind::McpServer,
                    canonical_name: credential_key.clone(),
                }
            }
            Self::WebProvider { provider, .. } => AuthorityPlace::ExternalAccount {
                provider: provider.clone(),
                account_id: provider.clone(),
            },
            Self::GitHubApp { app_id } => AuthorityPlace::ExternalAccount {
                provider: GITHUB_PROVIDER.to_string(),
                account_id: format!("app/{app_id}"),
            },
            Self::GitHubInstallation { installation_id } => AuthorityPlace::ExternalAccount {
                provider: GITHUB_PROVIDER.to_string(),
                account_id: format!("installation/{installation_id}"),
            },
            Self::ModelBackend {
                provider,
                account_id,
            } => AuthorityPlace::ExternalAccount {
                provider: provider.clone(),
                account_id: account_id.clone(),
            },
            Self::AccountDevice => AuthorityPlace::ExternalAccount {
                provider: CAIRN_CLOUD_PROVIDER.to_string(),
                account_id: "device".to_string(),
            },
            Self::TeamSync { team_id } => AuthorityPlace::ExternalAccount {
                provider: CAIRN_CLOUD_PROVIDER.to_string(),
                account_id: format!("team/{team_id}"),
            },
        };
        AuthorityScope::new(place, AuthorityAction::Run)
    }

    /// Stable non-secret identity for the registry and detection reports. Names
    /// the producer, never the value.
    pub(super) fn secret_id(&self) -> SecretId {
        SecretId::new(match self {
            Self::McpVar {
                credential_key,
                var,
            } => format!("mcp-server:{credential_key}:{var}"),
            Self::McpOAuth { credential_key } => format!("mcp-oauth:{credential_key}"),
            Self::WebProvider { provider, var } => format!("web-provider:{provider}:{var}"),
            Self::GitHubApp { app_id } => format!("github-app:{app_id}"),
            Self::GitHubInstallation { installation_id } => {
                format!("github-installation:{installation_id}")
            }
            Self::ModelBackend {
                provider,
                account_id,
            } => format!("model-backend:{provider}:{account_id}"),
            Self::AccountDevice => "account-device".to_string(),
            Self::TeamSync { team_id } => format!("team-sync:{team_id}"),
        })
    }

    fn category(&self) -> SecretCategory {
        match self {
            Self::McpVar { .. } => SecretCategory::ConfiguredMcp,
            Self::McpOAuth { .. } => SecretCategory::OAuthToken,
            Self::WebProvider { .. } => SecretCategory::ProviderKey,
            Self::GitHubApp { .. } => SecretCategory::ProviderSigningKey,
            Self::GitHubInstallation { .. } | Self::TeamSync { .. } => {
                SecretCategory::ProviderToken
            }
            Self::ModelBackend { .. } => SecretCategory::ModelBackendKey,
            Self::AccountDevice => SecretCategory::AccountToken,
        }
    }
}

/// A credential the broker resolved.
///
/// Deliberately missing: `Debug`, `Display`, `Serialize`, `Deserialize`,
/// `Clone`, and `PartialEq`. There is exactly one way to reach the bytes —
/// [`Self::expose`] — so every place plaintext leaves the broker is a named,
/// greppable call rather than an incidental `{:?}` or `to_string`.
/// `security::crossing` proves the absence of those impls at compile time.
pub struct BrokeredSecret {
    value: Zeroizing<String>,
    id: SecretId,
}

impl BrokeredSecret {
    /// The credential's plaintext, for injection into a transport.
    ///
    /// The deliberate exposure. The returned borrow belongs in an HTTP header,
    /// a child process environment, or a request body — never in a log line, a
    /// serializer, a database row, or anything a model observes.
    pub fn expose(&self) -> &str {
        &self.value
    }

    /// Non-secret identity, safe in logs and detection reports.
    pub fn id(&self) -> &SecretId {
        &self.id
    }
}

/// An MCP server configuration with every `${VAR}` resolved.
///
/// The reason this is a distinct type rather than an `McpServerConfig` is that
/// `McpServerConfig` is `Debug`, `Serialize`, and `Clone` — it is the *authored*
/// configuration type, and it has to be all three to live in `settings.yaml`.
/// An expanded config carries resolved credentials in `env`, `headers`, `url`,
/// and `args`, so wearing those impls is what let one get serialized into a
/// durable continuation row.
///
/// This carrier has none of them. It cannot be persisted, cloned into a
/// long-lived struct, or formatted; it can only be handed to a transport.
pub struct BrokeredMcpConfig {
    resolved: McpServerConfig,
}

impl BrokeredMcpConfig {
    /// The resolved configuration, for handing to an MCP transport.
    ///
    /// The deliberate exposure, and the one place an expanded config regains
    /// `Debug`/`Serialize`. The borrow is for connecting; storing it, logging
    /// it, or serializing it puts resolved credentials somewhere durable.
    pub fn resolved_for_connect(&self) -> &McpServerConfig {
        &self.resolved
    }

    /// The transport name (`stdio`, `http`, `sse`).
    ///
    /// Narrowly exposed because it is structurally an enum, not a value a
    /// credential can hide in, and rendering "which transport is this server"
    /// should not require handing out the whole resolved config.
    pub fn transport(&self) -> &str {
        &self.resolved.transport
    }
}

/// Where a resolved MCP variable came from. Decides the declared-secret signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    /// A value the operator typed into the settings UI's secret field on the way
    /// to storing it. Declared by the act of typing it there.
    Override,
    /// The OS keychain. Declared by the choice of store.
    Keychain,
    /// The process environment. Declared only if the server config says so.
    Environment,
}

/// Register a resolved credential and record the authority it was resolved
/// under. Returns the non-secret identity the value is registered under.
///
/// The single registration path. Every store-backed resolver in this module
/// funnels through it, and so does [`super::lease`] when it issues a lease, so
/// "was this registered before it was returned?" has one answer instead of one
/// per call site.
pub(super) fn register(
    source: &CredentialSource,
    declared: Declared,
    purpose: &str,
    value: String,
) -> SecretId {
    let secret_id = source.secret_id();
    if declared == Declared::Configuration {
        return secret_id;
    }
    let scope = source.scope();
    debug_assert_eq!(
        scope.action,
        AuthorityAction::Run,
        "the broker resolves credentials for use; reveal is not an authority it can mint"
    );
    // Provenance is the audit record: the CAIRN-3803 scope shorthand plus the
    // caller's stated purpose. Both are non-secret, and the purpose is carried
    // for an operator reading the inventory, never consulted to decide
    // anything.
    let provenance = format!("{} ({purpose})", scope.shorthand());
    match registry().register(
        secret_id.clone(),
        source.category(),
        provenance,
        SecretMaterial::from_string(value),
    ) {
        Ok(guard) => {
            // Held for the life of the process. A credential can appear in
            // observed output at any later moment — an MCP server may echo it
            // in a result long after the call that resolved it — so releasing
            // on scope exit would unregister while output carrying the value is
            // still in flight.
            guard.retain_for_process();
        }
        Err(error) => {
            // Loud and non-secret. A refusal means observed output is NOT
            // protected against this credential, which an operator needs to
            // know.
            log::warn!("credential {secret_id} not registered for scrubbing: {error}");
        }
    }
    secret_id
}

/// Read one MCP `${VAR}` from the OS keychain.
///
/// The only caller of the keychain accessor in the codebase; a source-structure
/// test keeps it that way, because a second caller is a second place a
/// credential becomes an unregistered `String`.
fn keychain(credential_key: &str, var: &str) -> Option<String> {
    crate::config::secrets::get_secret(credential_key, var)
}

/// Resolve one `${VAR}` for a configured MCP server: settings-UI override, then
/// the OS keychain, then the process environment.
///
/// This is the single resolution chain. Connect-time expansion, the readiness
/// probe, and the settings-save connection test all reach a `${VAR}` through
/// here, so there is one definition of what a reference resolves to and one
/// place that decides whether the result is registered.
fn resolve_mcp_var(
    credential_key: &str,
    authored: &McpServerConfig,
    overrides: &HashMap<String, String>,
    var: &str,
    purpose: &str,
) -> Option<String> {
    let (value, origin) = match overrides.get(var).filter(|value| !value.is_empty()) {
        Some(value) => (value.clone(), Origin::Override),
        None => match keychain(credential_key, var) {
            Some(value) => (value, Origin::Keychain),
            None => (std::env::var(var).ok()?, Origin::Environment),
        },
    };
    let declared = match origin {
        Origin::Override | Origin::Keychain => Declared::Secret,
        Origin::Environment if authored.declares_secret(var) => Declared::Secret,
        Origin::Environment => Declared::Configuration,
    };
    let source = CredentialSource::McpVar {
        credential_key: credential_key.to_string(),
        var: var.to_string(),
    };
    register(&source, declared, purpose, value.clone());
    Some(value)
}

/// Expand a configured MCP server's `${VAR}` references for connecting.
///
/// `overrides` carries values the operator typed in the settings UI but has not
/// stored yet, so a connection test exercises what is about to be saved. Pass an
/// empty map everywhere else.
///
/// `purpose` is audit metadata: it is recorded with the registration and is
/// never security input.
pub fn mcp_server(
    credential_key: &str,
    authored: &McpServerConfig,
    overrides: &HashMap<String, String>,
    purpose: &str,
) -> BrokeredMcpConfig {
    // A declaration that names nothing protects nothing, and the operator has no
    // way to tell: the whole value of the field is their belief that the
    // credential is scrubbed. A typo in `secrets:` would otherwise parse, save,
    // fingerprint, and silently do nothing.
    let referenced = authored.referenced_vars();
    for declared in &authored.secrets {
        if !referenced.contains(declared) {
            log::warn!(
                "MCP server `{credential_key}` declares `{declared}` a credential, but no \
                 ${{{declared}}} reference exists in its configuration. That declaration \
                 protects nothing."
            );
        }
    }
    let resolved = authored.expand_vars(&|var| {
        resolve_mcp_var(credential_key, authored, overrides, var, purpose).unwrap_or_default()
    });
    BrokeredMcpConfig { resolved }
}

/// Whether `var` resolves to a non-empty value for this server.
///
/// The readiness probe. It resolves through the same chain as connecting, so a
/// server reported ready is a server whose credentials actually resolve — and a
/// value materialized here is registered on the same terms as one materialized
/// at connect.
pub fn mcp_var_is_set(credential_key: &str, authored: &McpServerConfig, var: &str) -> bool {
    resolve_mcp_var(
        credential_key,
        authored,
        &HashMap::new(),
        var,
        "mcp readiness probe",
    )
    .is_some_and(|value| !value.trim().is_empty())
}

/// A currently-valid OAuth access token for a configured MCP server, refreshing
/// it when the stored one has expired.
///
/// The token is registered before it is returned, which matters more here than
/// for a static `${VAR}`: an OAuth bearer is attached to every request to a
/// server Cairn does not control, and a server that echoes its own
/// `Authorization` header back in an error body would otherwise put a live token
/// straight into a tool result.
pub async fn mcp_oauth_token(credential_key: &str, purpose: &str) -> Option<BrokeredSecret> {
    let value = crate::mcp::oauth::store::get_valid_access_token(credential_key).await?;
    let source = CredentialSource::McpOAuth {
        credential_key: credential_key.to_string(),
    };
    register(&source, Declared::Secret, purpose, value.clone());
    Some(BrokeredSecret {
        value: Zeroizing::new(value),
        id: source.secret_id(),
    })
}

/// The stored API key for a web search or web fetch provider.
///
/// Returns `None` when no key is stored or the stored value is blank, which is
/// the "provider not configured" case rather than a failure.
pub fn web_provider_key(provider: &str, var: &str, purpose: &str) -> Option<BrokeredSecret> {
    let credential_key = crate::config::secrets::credential_key(provider, None);
    let value = keychain(&credential_key, var).filter(|key| !key.trim().is_empty())?;
    let source = CredentialSource::WebProvider {
        provider: provider.to_string(),
        var: var.to_string(),
    };
    register(&source, Declared::Secret, purpose, value.clone());
    Some(BrokeredSecret {
        value: Zeroizing::new(value),
        id: source.secret_id(),
    })
}

/// GitHub App authentication, performed by the broker rather than handed out.
///
/// # The operation the broker performs
///
/// GitHub App authentication is a two-step exchange, and the two steps have
/// very different blast radii. The RSA private key authenticates Cairn *as the
/// application*: one signature mints a token for any repository the app is
/// installed on, it does not expire, and revoking it means generating a new key
/// on the app. The installation token it buys is scoped to one installation and
/// expires in an hour.
///
/// Before this module both steps ran in the caller's hands: `get_credentials_
/// for_owner` returned a struct with the private key in a `String` field, every
/// GitHub API function took it by reference, and model-callable handlers were
/// among the callers. The exchange has moved in here, so the key is read,
/// signed with, and dropped without crossing this module's boundary. A caller
/// gets a [`HeaderMap`] — the operation's result — and there is no entry point
/// that returns the key or the token as a `String`.
///
/// # What replaced the token cache
///
/// The old code kept installation tokens in a process-global `HashMap` with no
/// revocation: signing out of GitHub left every minted token live in memory
/// until it expired on its own. Tokens are now leases
/// ([`crate::security::lease`]), which is the same cache with a deadline, an
/// audience, and a revocation — so [`revoke_all`] is a thing disconnect can
/// call and mean.
pub mod github {
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, USER_AGENT};
    use serde::{Deserialize, Serialize};

    use crate::security::lease::{leases, CredentialLease, LeaseAudience, LeaseTerms};
    use crate::services::{HttpClient, HttpMethod, HttpResponse, RedirectTarget};
    use crate::storage::LocalDb;

    use super::CredentialSource;

    pub const API_BASE: &str = "https://api.github.com";
    const API_HOST: &str = "api.github.com";

    /// How long an app JWT is signed for. GitHub caps this at ten minutes.
    const APP_JWT_LIFETIME_SECONDS: i64 = 10 * 60;

    /// Re-mint a lease this long before it expires, so a credential is never
    /// first found to be stale by the provider rejecting it.
    const REFRESH_MARGIN_SECONDS: i64 = 300;

    /// The one audience any GitHub credential may be presented to.
    ///
    /// Used to *mint* a lease. Presenting one derives its audience from the URL
    /// of the request being sent instead — see [`authenticated_headers`].
    fn audience() -> LeaseAudience {
        LeaseAudience::https(API_HOST)
    }

    fn now() -> i64 {
        chrono::Utc::now().timestamp()
    }

    /// The non-credential headers every GitHub request carries.
    fn base_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.github+json"),
        );
        headers.insert(USER_AGENT, HeaderValue::from_static("Cairn"));
        headers.insert(
            "X-GitHub-Api-Version",
            HeaderValue::from_static("2022-11-28"),
        );
        headers
    }

    /// Attach a leased bearer to the headers for a request addressed to `url`.
    ///
    /// The deliberate exposure, and the only one in this module: a lease's
    /// plaintext becomes a header value and nothing else.
    ///
    /// The audience comes from the URL rather than from [`audience`], and that
    /// is the whole point of the function. Presenting to a constant the same
    /// module supplies is a check that cannot fail; presenting to the host the
    /// request will actually be sent to is a check that can. A lease minted for
    /// `api.github.com` is refused for a request addressed anywhere else, and
    /// refused for a plain-`http` URL, which would put the bearer on the wire
    /// in cleartext.
    fn authenticated_headers(lease: &CredentialLease, url: &str) -> Result<HeaderMap, String> {
        let parsed =
            reqwest::Url::parse(url).map_err(|error| format!("unusable GitHub URL: {error}"))?;
        if parsed.scheme() != "https" {
            return Err(format!(
                "refusing to send a GitHub credential over {}",
                parsed.scheme()
            ));
        }
        let host = parsed
            .host_str()
            .ok_or_else(|| "GitHub URL has no host to authenticate to".to_string())?;

        let presented = lease
            .present(&LeaseAudience::https(host))
            .map_err(|denied| {
                // Non-secret: names the lease, both audiences, and the deadline.
                format!("GitHub credential unusable: {denied}")
            })?;
        let mut headers = base_headers();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", presented.expose()))
                .map_err(|error| error.to_string())?,
        );
        Ok(headers)
    }

    /// How many redirects the broker will follow before giving up.
    ///
    /// Small on purpose. GitHub's own redirects are a single hop — a renamed
    /// repository, or a log download pointing at storage — so a longer chain is
    /// a loop or a server misbehaving, not a case worth serving.
    const MAX_REDIRECTS: usize = 5;

    /// Send one authenticated request, deciding every redirect explicitly.
    ///
    /// Credential and destination are bound in a single call: the URL a request
    /// is sent to is the same URL whose host unlocked the lease.
    ///
    /// This is why the authorities below expose request methods rather than a
    /// `headers()` accessor. A `HeaderMap` handed back to a caller is a bearer
    /// that has *already* passed its audience check and can then be attached to
    /// any URL at all — which makes the check a formality and the audience a
    /// comment. Nothing in this module returns one.
    ///
    /// # Why redirects are handled here
    ///
    /// A redirect is the same hole from the other side. The transport does not
    /// follow them (see [`crate::services::RealHttpClient`]) precisely because
    /// following one is the decision to resend a credential to a URL that
    /// nothing checked — an audience validated against the URL a caller named
    /// means nothing if the transport then repeats the request somewhere else.
    /// So the decision is made here, under one rule: **a credential is attached
    /// only to a URL that has just passed [`authenticated_headers`]**, the first
    /// hop and every later one alike.
    ///
    /// Three behaviours follow from that rule:
    ///
    /// - A redirect that stays on GitHub over HTTPS is revalidated and followed
    ///   with the credential. This is how a renamed repository still resolves.
    /// - A redirect that leaves GitHub is followed *without* the credential, and
    ///   once dropped it is never re-attached, even if a later hop points back
    ///   at GitHub. This is what makes workflow log downloads work: GitHub
    ///   answers `/logs` with a redirect to blob storage carrying its own signed
    ///   token, and sending a GitHub bearer there would hand a storage provider
    ///   an installation credential.
    /// - A redirect to plain `http` is refused outright. That is the case a
    ///   library's own stripping misses, because the usual test compares host
    ///   and port and a downgrade changes neither.
    ///
    /// Only a GET is followed. A redirected write would either replay its body
    /// against a server that did not accept the first one or silently degrade to
    /// a GET, and neither is a thing to do quietly with a mutation.
    ///
    /// # The target is itself a capability
    ///
    /// The URL a provider redirects to can carry its authorization in its query
    /// string — GitHub's log download points at blob storage with a `?sig=`
    /// token that is the whole credential for that object. So the current
    /// destination is held as a [`RedirectTarget`] rather than a `String` for
    /// the whole loop, including the caller's own first URL. Keeping one type
    /// throughout means there is no point in this function where a bare URL is
    /// sitting in scope waiting to be interpolated into a message, which is how
    /// the leak would come back.
    async fn send(
        http: &dyn HttpClient,
        lease: &CredentialLease,
        method: HttpMethod,
        url: &str,
        body: serde_json::Value,
    ) -> Result<HttpResponse, String> {
        let mut current = RedirectTarget::new(url);
        // Goes false when a hop leaves GitHub, and never goes back.
        let mut credentialed = true;

        for _ in 0..=MAX_REDIRECTS {
            let headers = if credentialed {
                authenticated_headers(lease, current.as_str())?
            } else {
                base_headers()
            };
            let url = current.as_str();
            let response = match method {
                HttpMethod::Get => http.get(url, headers).await?,
                HttpMethod::Post => http.post(url, body.clone(), headers).await?,
                HttpMethod::Put => http.put(url, body.clone(), headers).await?,
                HttpMethod::Patch => http.patch(url, body.clone(), headers).await?,
                HttpMethod::Delete => http.delete(url, headers).await?,
            };

            let target = response.redirect_target().cloned();
            let Some(target) = target else {
                return Ok(response);
            };
            if method != HttpMethod::Get {
                return Err(format!(
                    "refusing to follow a redirect on a {} to {}",
                    method.label(),
                    current.summary()
                ));
            }

            let next = reqwest::Url::parse(current.as_str())
                .and_then(|base| base.join(target.as_str()))
                .map_err(|error| format!("unusable redirect from GitHub: {error}"))?;
            if next.scheme() != "https" {
                return Err(format!(
                    "refusing to follow a GitHub redirect to {}",
                    next.scheme()
                ));
            }
            credentialed = credentialed && next.host_str() == Some(API_HOST);
            current = RedirectTarget::new(next);
        }

        Err(format!(
            "too many redirects from GitHub for {}",
            current.summary()
        ))
    }

    #[derive(Debug, Serialize)]
    struct JwtClaims {
        iat: i64,
        exp: i64,
        iss: i64,
    }

    /// Authority to act as the GitHub App itself.
    ///
    /// Carries a lease over a signed JWT, never the key that signed it. No
    /// `Debug`, no `serde`, no `Clone` beyond what the lease itself allows.
    pub struct AppAuthority {
        app_id: i64,
        jwt: CredentialLease,
    }

    impl AppAuthority {
        pub fn app_id(&self) -> i64 {
            self.app_id
        }

        /// Send a GET authenticated as the app.
        pub async fn get(&self, http: &dyn HttpClient, url: &str) -> Result<HttpResponse, String> {
            send(
                http,
                &self.jwt,
                HttpMethod::Get,
                url,
                serde_json::Value::Null,
            )
            .await
        }

        /// Send a PATCH authenticated as the app.
        pub async fn patch(
            &self,
            http: &dyn HttpClient,
            url: &str,
            body: serde_json::Value,
        ) -> Result<HttpResponse, String> {
            send(http, &self.jwt, HttpMethod::Patch, url, body).await
        }

        /// Send a DELETE authenticated as the app.
        pub async fn delete(
            &self,
            http: &dyn HttpClient,
            url: &str,
        ) -> Result<HttpResponse, String> {
            send(
                http,
                &self.jwt,
                HttpMethod::Delete,
                url,
                serde_json::Value::Null,
            )
            .await
        }
    }

    /// Sign an app JWT and lease it.
    ///
    /// Split from [`app_authority`] so the signing step is reachable in tests
    /// with a fixture key: the store read is what the source-structure ban
    /// keeps in this module, and it lives in the caller.
    pub(crate) fn app_authority_from_key(
        app_id: i64,
        private_key: &str,
    ) -> Result<AppAuthority, String> {
        let source = CredentialSource::GitHubApp { app_id };
        let issued = now();
        let expires_at = issued + APP_JWT_LIFETIME_SECONDS;

        if let Some(jwt) = leases().live(&source, &audience(), issued + REFRESH_MARGIN_SECONDS) {
            return Ok(AppAuthority { app_id, jwt });
        }

        let claims = JwtClaims {
            // GitHub rejects a JWT whose `iat` is in its future; back-date to
            // absorb clock skew between this machine and theirs.
            iat: issued - 60,
            exp: expires_at,
            iss: app_id,
        };
        let key = EncodingKey::from_rsa_pem(private_key.as_bytes())
            .map_err(|error| format!("Invalid private key: {error}"))?;
        let jwt = encode(&Header::new(Algorithm::RS256), &claims, &key)
            .map_err(|error| format!("Failed to generate JWT: {error}"))?;

        let jwt = leases().issue(
            LeaseTerms {
                source: &source,
                audience: audience(),
                expires_at,
                purpose: "github app authentication",
            },
            jwt,
        );
        Ok(AppAuthority { app_id, jwt })
    }

    /// Authenticate as the GitHub App.
    ///
    /// Reads the stored signing key, signs, and drops it. The key does not
    /// leave this function.
    pub async fn app_authority(db: &LocalDb) -> Result<AppAuthority, String> {
        let (app_id, private_key) = crate::github::credentials::app_signing_key(db).await?;
        // Registered as well as signed with: the key never travels, but a
        // provider that echoed it into an error body would otherwise put it
        // straight into observed output.
        super::register(
            &CredentialSource::GitHubApp { app_id },
            super::Declared::Secret,
            "github app signing key",
            private_key.to_string(),
        );
        app_authority_from_key(app_id, &private_key)
    }

    /// Authority to act as one GitHub App installation.
    ///
    /// Carries the app's signed JWT, never the key that signed it, and mints
    /// the installation token from that signature on demand. Two leases deep,
    /// and the store-held credential is behind both.
    pub struct InstallationAuthority {
        app: AppAuthority,
        installation_id: i64,
    }

    impl InstallationAuthority {
        pub fn installation_id(&self) -> i64 {
            self.installation_id
        }

        pub fn app_id(&self) -> i64 {
            self.app.app_id
        }

        /// Send a GET authenticated as this installation.
        pub async fn get(&self, http: &dyn HttpClient, url: &str) -> Result<HttpResponse, String> {
            self.send(http, HttpMethod::Get, url, serde_json::Value::Null)
                .await
        }

        /// Send a PUT authenticated as this installation.
        pub async fn put(
            &self,
            http: &dyn HttpClient,
            url: &str,
            body: serde_json::Value,
        ) -> Result<HttpResponse, String> {
            self.send(http, HttpMethod::Put, url, body).await
        }

        /// Send a PATCH authenticated as this installation.
        pub async fn patch(
            &self,
            http: &dyn HttpClient,
            url: &str,
            body: serde_json::Value,
        ) -> Result<HttpResponse, String> {
            self.send(http, HttpMethod::Patch, url, body).await
        }

        /// Send a DELETE authenticated as this installation.
        pub async fn delete(
            &self,
            http: &dyn HttpClient,
            url: &str,
        ) -> Result<HttpResponse, String> {
            self.send(http, HttpMethod::Delete, url, serde_json::Value::Null)
                .await
        }

        /// Mint or reuse the installation's token lease, then send.
        async fn send(
            &self,
            http: &dyn HttpClient,
            method: HttpMethod,
            url: &str,
            body: serde_json::Value,
        ) -> Result<HttpResponse, String> {
            let token = self.token(http).await?;
            send(http, &token, method, url, body).await
        }

        async fn token(&self, http: &dyn HttpClient) -> Result<CredentialLease, String> {
            let source = CredentialSource::GitHubInstallation {
                installation_id: self.installation_id,
            };
            if let Some(live) = leases().live(&source, &audience(), now() + REFRESH_MARGIN_SECONDS)
            {
                return Ok(live);
            }

            let url = format!(
                "{API_BASE}/app/installations/{}/access_tokens",
                self.installation_id
            );
            let response = send(
                http,
                &self.app.jwt,
                HttpMethod::Post,
                &url,
                serde_json::json!({}),
            )
            .await?;
            if !response.is_success() {
                return Err(format!(
                    "GitHub API error: {} - {}",
                    response.status,
                    response.text()
                ));
            }
            let minted: InstallationTokenResponse = response.json()?;
            // A token whose expiry we cannot read is a token we cannot lease
            // honestly. Treating an unparseable deadline as "expires now" would
            // refuse it immediately, and as "never" would defeat the lease, so
            // fall back to GitHub's documented one-hour lifetime.
            let expires_at = chrono::DateTime::parse_from_rfc3339(&minted.expires_at)
                .map(|at| at.timestamp())
                .unwrap_or_else(|_| now() + 3600);

            Ok(leases().issue(
                LeaseTerms {
                    source: &source,
                    audience: audience(),
                    expires_at,
                    purpose: "github installation api call",
                },
                minted.token,
            ))
        }
    }

    #[derive(Debug, Deserialize)]
    struct InstallationTokenResponse {
        token: String,
        expires_at: String,
    }

    /// Authority for the installation covering `owner`, falling back to the
    /// default installation.
    pub async fn installation_authority(
        db: &LocalDb,
        owner: &str,
    ) -> Result<InstallationAuthority, String> {
        let (_, installation_id) = crate::github::credentials::installation_identity(db, owner)
            .await
            .map_err(|error| error.to_string())?;
        Ok(InstallationAuthority {
            app: app_authority(db).await?,
            installation_id,
        })
    }

    /// An installation authority built from a signing key already in hand.
    ///
    /// Exists so tests can exercise GitHub request handling against a mock
    /// transport with a fixture key and no credential store. Production always
    /// arrives through [`installation_authority`], which reads the stored key
    /// and drops it.
    #[cfg(test)]
    pub(crate) fn installation_authority_from_key(
        app_id: i64,
        installation_id: i64,
        private_key: &str,
    ) -> Result<InstallationAuthority, String> {
        Ok(InstallationAuthority {
            app: app_authority_from_key(app_id, private_key)?,
            installation_id,
        })
    }

    /// Revoke every live lease presentable to GitHub. Returns how many.
    ///
    /// What disconnecting the GitHub App should mean: not just that the stored
    /// key is gone, but that every token already minted from it stops working
    /// in this process, whoever is holding one.
    pub fn revoke_all() -> usize {
        leases().revoke_audience(&audience())
    }

    /// Revoke live leases derived from one installation. Returns how many.
    pub fn revoke_installation(installation_id: i64) -> usize {
        leases().revoke_source(&CredentialSource::GitHubInstallation { installation_id })
    }
}

/// This desktop's own account credentials.
pub mod account {
    use crate::security::lease::{leases, CredentialLease, LeaseAudience, LeaseTerms};

    use super::{register, CredentialSource, Declared};

    /// The consumer role a team sync token is leased to.
    pub const SYNC_ROLE: &str = "team-sync";

    /// Register this device's account credential.
    ///
    /// Registration rather than a lease, and the difference is where the
    /// revocation already lives: the JWT is re-read from the private database
    /// at every use, so signing out — which deletes the row — already stops it
    /// being handed out. What was missing was scrubbing. The device JWT is the
    /// credential behind every authenticated call this desktop makes, and until
    /// now it could appear verbatim in observed output.
    pub fn device_credential(value: &str) {
        register(
            &CredentialSource::AccountDevice,
            Declared::Secret,
            "cairn account device credential",
            value.to_string(),
        );
    }

    /// A live team sync token lease, if one is still good at `not_before`.
    pub fn live_sync_token(team_id: &str, not_before: i64) -> Option<CredentialLease> {
        leases().live(&sync_source(team_id), &sync_audience(), not_before)
    }

    /// Lease a freshly minted team sync token.
    pub fn lease_sync_token(team_id: &str, expires_at: i64, token: String) -> CredentialLease {
        leases().issue(
            LeaseTerms {
                source: &sync_source(team_id),
                audience: sync_audience(),
                expires_at,
                purpose: "team database sync",
            },
            token,
        )
    }

    /// The audience a sync token may be presented to.
    pub fn sync_audience() -> LeaseAudience {
        LeaseAudience::process(SYNC_ROLE)
    }

    /// Revoke every live team sync token. Returns how many.
    ///
    /// Called whenever the account row changes — signed out, replaced by a
    /// different user, or its team memberships rewritten. A sync token is
    /// derived from the device credential, so it must not outlive the account
    /// that produced it, and it must not survive a team the operator has left.
    ///
    /// Deliberately all teams rather than a computed difference: the next mint
    /// re-derives a token for every team still joined, at one round-trip each,
    /// and a diff that got the removed set wrong would fail in the direction
    /// that leaves a credential live.
    pub fn revoke_all_sync_tokens() -> usize {
        leases().revoke_audience(&sync_audience())
    }

    fn sync_source(team_id: &str) -> CredentialSource {
        CredentialSource::TeamSync {
            team_id: team_id.to_string(),
        }
    }
}

/// Model backend credentials, leased to the agent process that reads them.
///
/// # The case the broker cannot make disappear
///
/// Everywhere else in this module the goal is that plaintext never leaves: the
/// broker signs, or performs the call, and hands back a result. That is not
/// available here. The Claude and Codex CLIs read their credential out of their
/// own process environment, and no amount of brokering changes what a program
/// Cairn did not write expects to find in `ANTHROPIC_API_KEY`. Once the value is
/// in that environment the consuming process holds it, and the blast radius is
/// whatever that process can reach.
///
/// So this module does not claim to remove the exposure. It does two things
/// that were not being done at all:
///
/// 1. **Registers before injecting.** Until now a backend key reached the agent
///    environment without ever becoming a scrub target, so an agent that ran
///    `env` — or a CLI that echoed its configuration into an error — put the
///    operator's own API key straight into the transcript. Registration closes
///    that, and it is the larger of the two wins.
/// 2. **Records the exposure as a lease.** An injection that used to be
///    invisible now appears in the lease inventory with an audience naming the
///    process role and a deadline. An operator can see that the exposure exists
///    and when it was made.
///
/// The deadline here bounds *re-issuance*, not the consuming process's
/// possession: expiry means Cairn re-reads the identity store rather than
/// handing out the same cached value, so a rotated key propagates within the
/// window. It does not reach into a running agent and take anything back, and
/// nothing in this module pretends otherwise.
pub mod backend {
    use crate::security::lease::{leases, LeaseAudience, LeaseTerms};

    use super::{CredentialSource, Declared};

    /// The process role a Claude agent's credential is leased to.
    pub const CLAUDE_ROLE: &str = "claude-agent";
    /// The process role a Codex agent's credential is leased to.
    pub const CODEX_ROLE: &str = "codex-agent";

    /// How long a backend credential lease is reused before the identity store
    /// is read again. Short enough that a rotated key propagates within the
    /// hour, long enough that a busy runner is not re-reading per spawn.
    const LEASE_SECONDS: i64 = 3600;

    /// Register a model backend credential and lease it to the agent process
    /// about to read it, returning the value for that process's environment.
    ///
    /// `provider` and `account_id` name the account in the audit record;
    /// neither is a secret. `role` is the process audience, and presenting to
    /// anything else is refused.
    pub fn agent_credential(
        provider: &str,
        account_id: &str,
        role: &str,
        value: &str,
    ) -> Result<String, String> {
        let source = CredentialSource::ModelBackend {
            provider: provider.to_string(),
            account_id: account_id.to_string(),
        };
        let audience = LeaseAudience::process(role);
        let now = chrono::Utc::now().timestamp();

        let lease = match leases().live(&source, &audience, now) {
            Some(live) => live,
            None => leases().issue(
                LeaseTerms {
                    source: &source,
                    audience: audience.clone(),
                    expires_at: now + LEASE_SECONDS,
                    purpose: "agent process credential",
                },
                value.to_string(),
            ),
        };

        // The deliberate exposure, and the reason this function returns a
        // `String` where the rest of the broker refuses to: an environment
        // variable is a string, and this is the boundary where that becomes
        // true.
        let presented = lease
            .present(&audience)
            .map_err(|denied| format!("backend credential unusable: {denied}"))?;
        Ok(presented.expose().to_string())
    }

    /// Revoke every backend credential leased to one process role.
    pub fn revoke_role(role: &str) -> usize {
        leases().revoke_audience(&LeaseAudience::process(role))
    }

    /// Register a credential that is injected without a lease.
    ///
    /// For a credential whose delivery Cairn does not control well enough to
    /// lease — one written into a file a CLI reads, rather than handed over at
    /// a call site. Registration still applies, so the value is scrubbed from
    /// observed output even where the lease's audience check has nothing to
    /// bind to.
    pub fn register_injected(provider: &str, account_id: &str, value: &str) {
        super::register(
            &CredentialSource::ModelBackend {
                provider: provider.to_string(),
                account_id: account_id.to_string(),
            },
            Declared::Secret,
            "agent process credential (unleased injection)",
            value.to_string(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::secrets::mock_keychain::install as use_mock_keychain;
    use crate::security::{registry, Sanitizer};

    fn authored(secrets: &[&str]) -> McpServerConfig {
        serde_json::from_value(serde_json::json!({ "secrets": secrets })).unwrap()
    }

    fn is_registered(id: &str) -> bool {
        registry()
            .metadata()
            .iter()
            .any(|entry| entry.id.as_str() == id)
    }

    /// The keychain branch registers, as it always has.
    #[test]
    fn a_keychain_value_is_declared_by_its_store() {
        use_mock_keychain();
        let (server, var) = ("broker-keychain-server", "KEYCHAIN_TOKEN");
        let value = "kc-Zx91Qw82Lm73Pv";
        crate::config::secrets::set_secret(server, var, value).unwrap();

        let resolved = resolve_mcp_var(server, &authored(&[]), &HashMap::new(), var, "test");
        assert_eq!(resolved.as_deref(), Some(value));
        assert!(is_registered(&format!("mcp-server:{server}:{var}")));
    }

    /// The gap CAIRN-3822 deliberately left open: an environment-resolved
    /// credential the config declares is now a scrub target.
    #[test]
    fn a_declared_environment_value_is_registered() {
        let (server, var) = ("broker-declared-server", "CAIRN_TEST_DECLARED_TOKEN");
        let value = "env-Rt48Km19Zc03Qb";
        // SAFETY: a process-unique variable name, set once and never removed, so
        // no concurrent test observes a different value for it.
        unsafe { std::env::set_var(var, value) };

        let resolved = resolve_mcp_var(server, &authored(&[var]), &HashMap::new(), var, "test");
        assert_eq!(resolved.as_deref(), Some(value));
        assert!(is_registered(&format!("mcp-server:{server}:{var}")));

        let mut sanitizer = Sanitizer::exact();
        assert!(!sanitizer
            .text(&format!("the server echoed {value}"))
            .contains(value));
    }

    /// And the reason that signal has to be declared rather than inferred: an
    /// undeclared environment value is ordinary configuration, and registering
    /// it would make every model-authored write mentioning it refused.
    #[test]
    fn an_undeclared_environment_value_resolves_without_being_registered() {
        let (server, var) = ("broker-plain-server", "CAIRN_TEST_UNDECLARED_PATH");
        let value = "/Users/somebody/Documents";
        // SAFETY: as above.
        unsafe { std::env::set_var(var, value) };

        let resolved = resolve_mcp_var(server, &authored(&[]), &HashMap::new(), var, "test");
        assert_eq!(
            resolved.as_deref(),
            Some(value),
            "expansion must still work"
        );
        assert!(
            !is_registered(&format!("mcp-server:{server}:{var}")),
            "an undeclared environment value must not become a process-global scrub target"
        );
    }

    /// A value the operator typed into the settings UI, on its way to the
    /// keychain, is a secret before it gets there.
    #[test]
    fn a_settings_override_is_declared() {
        let (server, var) = ("broker-override-server", "OVERRIDE_TOKEN");
        let value = "ovr-Hj62Nq04Ws88Tz";
        let overrides = HashMap::from([(var.to_string(), value.to_string())]);

        let resolved = resolve_mcp_var(server, &authored(&[]), &overrides, var, "test");
        assert_eq!(resolved.as_deref(), Some(value));
        assert!(is_registered(&format!("mcp-server:{server}:{var}")));
    }

    /// The override wins over a stored value, so a connection test exercises
    /// what is about to be saved rather than what is already there.
    #[test]
    fn an_override_shadows_the_stored_value() {
        use_mock_keychain();
        let (server, var) = ("broker-shadow-server", "SHADOW_TOKEN");
        crate::config::secrets::set_secret(server, var, "stored-Ab12Cd34Ef").unwrap();
        let overrides = HashMap::from([(var.to_string(), "typed-Gh56Ij78Kl".to_string())]);

        assert_eq!(
            resolve_mcp_var(server, &authored(&[]), &overrides, var, "test").as_deref(),
            Some("typed-Gh56Ij78Kl")
        );
    }

    /// Every scope the broker mints is a *use* authority. Reveal is not
    /// expressible here, which is the point.
    ///
    /// The list is every variant of [`CredentialSource`] deliberately, so
    /// adding a producer without deciding what authority it exercises fails to
    /// compile rather than passing silently.
    #[test]
    fn every_brokered_scope_is_a_use_authority() {
        for source in [
            CredentialSource::McpVar {
                credential_key: "linear".into(),
                var: "API_KEY".into(),
            },
            CredentialSource::McpOAuth {
                credential_key: "linear".into(),
            },
            CredentialSource::WebProvider {
                provider: "tavily".into(),
                var: "API_KEY".into(),
            },
            CredentialSource::GitHubApp { app_id: 42 },
            CredentialSource::GitHubInstallation {
                installation_id: 99,
            },
            CredentialSource::ModelBackend {
                provider: "anthropic".into(),
                account_id: "someone@example.com".into(),
            },
            CredentialSource::AccountDevice,
            CredentialSource::TeamSync {
                team_id: "team-1".into(),
            },
        ] {
            assert_eq!(
                source.scope().action,
                AuthorityAction::Run,
                "{source:?} must be a use authority, not a reveal"
            );
        }
    }

    /// The scope names the server the credential actually belongs to, so an
    /// audit record cannot attribute one server's token to another.
    #[test]
    fn the_scope_names_the_resolved_target() {
        let source = CredentialSource::McpVar {
            credential_key: "linear".into(),
            var: "API_KEY".into(),
        };
        assert!(
            source.scope().shorthand().contains("linear"),
            "{}",
            source.scope().shorthand()
        );
    }

    /// Each producer gets its own registry identity, so a detection report can
    /// say which credential leaked. Two producers sharing an id would make one
    /// registration silently replace the other's stored forms.
    #[test]
    fn every_credential_source_has_its_own_identity() {
        let sources = [
            CredentialSource::GitHubApp { app_id: 42 },
            CredentialSource::GitHubInstallation {
                installation_id: 42,
            },
            CredentialSource::ModelBackend {
                provider: "anthropic".into(),
                account_id: "someone@example.com".into(),
            },
            CredentialSource::AccountDevice,
            CredentialSource::TeamSync {
                team_id: "42".into(),
            },
        ];
        let ids: std::collections::HashSet<_> = sources
            .iter()
            .map(|source| source.secret_id().as_str().to_string())
            .collect();
        assert_eq!(
            ids.len(),
            sources.len(),
            "credential sources must not share a registry identity: {ids:?}"
        );
    }

    mod github_operations {
        use super::*;
        use crate::security::lease::LeaseAudience;
        use crate::services::testing::MockHttpClient;
        use crate::services::HttpResponse;

        const TEST_KEY: &str = include_str!("../../tests/fixtures/test_rsa_key.pem");

        /// Assert that nothing addressed outside `https://api.github.com`
        /// carried a credential.
        ///
        /// The shared assertion for every redirect test below, because the
        /// property under test is always the same one: a request that did not
        /// pass the audience check must not be an authenticated request.
        fn assert_no_credential_left_github(http: &MockHttpClient) {
            for request in http.requests() {
                if request.url.starts_with("https://api.github.com") {
                    continue;
                }
                assert!(
                    !request.is_authenticated(),
                    "a credential was sent to {} — {:?}",
                    request.url,
                    request.authorization(),
                );
            }
        }

        /// The signed JWT is leased, and the lease is a scrub target — so an
        /// app signature echoed back by a provider does not reach observed
        /// output. Signing inside the broker is what keeps the RSA key from
        /// ever being the thing a caller holds.
        #[test]
        fn a_signed_app_jwt_is_leased_and_scrubbed() {
            let app = github::app_authority_from_key(918_273, TEST_KEY).unwrap();
            assert_eq!(app.app_id(), 918_273);

            let lease = crate::security::lease::leases()
                .live(
                    &CredentialSource::GitHubApp { app_id: 918_273 },
                    &LeaseAudience::https("api.github.com"),
                    chrono::Utc::now().timestamp(),
                )
                .expect("the signature is leased");
            let jwt = {
                let presented = lease
                    .present(&LeaseAudience::https("api.github.com"))
                    .expect("a fresh signature is presentable");
                presented.expose().to_string()
            };

            let mut sanitizer = crate::security::Sanitizer::exact();
            assert!(!sanitizer.text(&format!("echoed {jwt}")).contains(&jwt));
        }

        /// A redirect that leaves GitHub is followed *without* the credential.
        ///
        /// This is the workflow-log shape, and the reason the redirect handling
        /// exists at all: GitHub answers `/logs` with a redirect to blob
        /// storage that carries its own signed token. Before the transport
        /// stopped following redirects, this hop was taken by reqwest with
        /// whatever headers it had been handed, and whether the installation
        /// bearer went along was decided by a dependency's heuristic rather
        /// than by anything in this repository.
        #[tokio::test]
        async fn a_redirect_off_github_is_followed_without_the_credential() {
            let http = MockHttpClient::new()
                .respond_to(
                    "api.github.com/repos/o/r/actions/runs/7/logs",
                    HttpResponse::redirect(
                        302,
                        "https://productionresultssa.blob.core.windows.net/actions/7?sig=signed",
                    ),
                )
                .respond_to(
                    "blob.core.windows.net",
                    HttpResponse::new(200, b"log bytes".to_vec()),
                );

            let app = github::app_authority_from_key(515_151, TEST_KEY).unwrap();
            let response = app
                .get(
                    &http,
                    "https://api.github.com/repos/o/r/actions/runs/7/logs",
                )
                .await
                .expect("the redirect is followed to storage");

            // The redirect was followed, so the caller still gets its logs.
            assert_eq!(response.status, 200);
            assert_eq!(response.body, b"log bytes");

            let requests = http.requests();
            assert_eq!(requests.len(), 2, "one hop to GitHub, one to storage");
            assert!(
                requests[0].is_authenticated(),
                "the request to GitHub authenticates"
            );
            assert!(
                !requests[1].is_authenticated(),
                "the request to storage must carry no credential"
            );
            assert_no_credential_left_github(&http);
        }

        /// A redirect target's signed query never reaches observed output.
        ///
        /// The second half of the redirect problem, and the subtler half. The
        /// bearer is not the only credential in play once a hop leaves GitHub:
        /// the *target itself* carries one, because storage providers sign a
        /// URL's query and that signature is the entire authorization for the
        /// object. Dropping the `Authorization` header and then printing the
        /// URL would trade one credential for another.
        ///
        /// The chain here redirects to itself so the hop limit is exhausted,
        /// which is the path that formats the current destination into an
        /// error. The sentinel must appear in neither that error nor the
        /// `Debug` of the response that carried it — `{:?}` on a response is at
        /// least as likely a route into a log as a hand-written message.
        #[tokio::test]
        async fn a_redirect_targets_signature_never_reaches_an_error() {
            const SENTINEL: &str = "s1gnatur3-that-must-not-appear";

            let signed = format!("https://storage.example.com/actions/7?sig={SENTINEL}");
            let http = MockHttpClient::new()
                .respond_to(
                    "api.github.com/repos/o/r/actions/runs/7/logs",
                    HttpResponse::redirect(302, &signed),
                )
                // Storage redirects to itself, so the loop runs out of hops
                // while its destination is the signed URL.
                .respond_to("storage.example.com", HttpResponse::redirect(302, &signed));

            let app = github::app_authority_from_key(515_155, TEST_KEY).unwrap();
            let Err(error) = app
                .get(
                    &http,
                    "https://api.github.com/repos/o/r/actions/runs/7/logs",
                )
                .await
            else {
                panic!("an endless redirect chain must not resolve");
            };

            assert!(
                error.contains("too many redirects"),
                "the failure is still legible: {error}"
            );
            assert!(
                !error.contains(SENTINEL),
                "a signed redirect target reached an error: {error}"
            );
            assert!(
                error.contains("storage.example.com"),
                "the host is still named, so the error says where: {error}"
            );

            // The same value must not escape through the response it arrived
            // on, which is why `RedirectTarget` overrides `Debug` rather than
            // deriving it.
            let carrier = format!("{:?}", HttpResponse::redirect(302, &signed));
            assert!(
                !carrier.contains(SENTINEL),
                "a signed redirect target reached a Debug rendering: {carrier}"
            );

            assert_no_credential_left_github(&http);
        }

        /// A same-host downgrade to plain `http` is refused.
        ///
        /// The case a library's own header-stripping misses: the usual test for
        /// "is this redirect cross-origin" compares host and port, and a
        /// downgrade changes neither, so the bearer would have gone out in
        /// cleartext to the same host it was minted for.
        #[tokio::test]
        async fn a_redirect_downgrading_to_plain_http_is_refused() {
            let http = MockHttpClient::new().respond_to(
                "https://api.github.com/app/installations",
                HttpResponse::redirect(302, "http://api.github.com/app/installations"),
            );

            let app = github::app_authority_from_key(515_152, TEST_KEY).unwrap();
            let Err(error) = app
                .get(&http, "https://api.github.com/app/installations")
                .await
            else {
                panic!("a downgrade to cleartext must not be followed");
            };
            assert!(error.contains("refusing to follow"), "{error}");

            let requests = http.requests();
            assert_eq!(requests.len(), 1, "the downgraded hop is never sent");
            assert!(
                requests
                    .iter()
                    .all(|request| !request.url.starts_with("http:")),
                "nothing was sent over cleartext"
            );
            assert_no_credential_left_github(&http);
        }

        /// A redirect that stays on GitHub over HTTPS is revalidated and
        /// followed with the credential, so a renamed repository still
        /// resolves. Refusing every redirect would be safe and would also
        /// silently break this.
        #[tokio::test]
        async fn a_redirect_within_github_is_revalidated_and_followed() {
            let http = MockHttpClient::new()
                .respond_to(
                    "/repos/old/name",
                    HttpResponse::redirect(301, "https://api.github.com/repos/new/name"),
                )
                .respond_to(
                    "/repos/new/name",
                    HttpResponse::new(200, br#"{"full_name":"new/name"}"#.to_vec()),
                );

            let app = github::app_authority_from_key(515_153, TEST_KEY).unwrap();
            let response = app
                .get(&http, "https://api.github.com/repos/old/name")
                .await
                .expect("a rename still resolves");
            assert_eq!(response.status, 200);

            let requests = http.requests();
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[1].url, "https://api.github.com/repos/new/name");
            assert!(
                requests[1].is_authenticated(),
                "a hop that passed the audience check again may authenticate"
            );
        }

        /// A redirected write is refused rather than replayed or quietly
        /// degraded to a GET.
        #[tokio::test]
        async fn a_redirect_on_a_write_is_refused() {
            let http = MockHttpClient::new().respond_to(
                "/app/hook/config",
                HttpResponse::redirect(307, "https://api.github.com/elsewhere"),
            );

            let app = github::app_authority_from_key(515_154, TEST_KEY).unwrap();
            let Err(error) = app
                .patch(
                    &http,
                    "https://api.github.com/app/hook/config",
                    serde_json::json!({"url": "https://example.com"}),
                )
                .await
            else {
                panic!("a mutation must not be replayed at a redirect target");
            };
            assert!(error.contains("PATCH"), "{error}");
            assert_eq!(http.requests().len(), 1, "the redirect is not followed");
        }

        /// The audience check has to be a real check, which means the host it
        /// compares against comes from the request being sent rather than from
        /// a constant the same module supplies. A request addressed anywhere
        /// but GitHub is refused *before* it goes out, and the mock transport
        /// proves the refusal happened first by never being called.
        #[tokio::test]
        async fn an_authority_refuses_a_request_addressed_elsewhere() {
            let app = github::app_authority_from_key(424_242, TEST_KEY).unwrap();
            let http = MockHttpClient::new();

            let Err(error) = app
                .get(&http, "https://attacker.example.com/app/installations")
                .await
            else {
                panic!("a GitHub credential must not be sent to another host");
            };
            assert!(error.contains("unusable"), "{error}");
            assert!(
                !error.contains("Bearer"),
                "the refusal must not carry the credential: {error}"
            );
        }

        /// A GitHub URL over plain `http` would put the bearer on the wire in
        /// cleartext. Refused for the same reason and in the same place.
        #[tokio::test]
        async fn an_authority_refuses_to_authenticate_over_plain_http() {
            use crate::services::testing::MockHttpClient;

            let app = github::app_authority_from_key(424_243, TEST_KEY).unwrap();
            let http = MockHttpClient::new();

            let Err(error) = app
                .get(&http, "http://api.github.com/app/installations")
                .await
            else {
                panic!("a GitHub credential must not be sent in cleartext");
            };
            assert!(error.contains("refusing to send"), "{error}");
        }

        /// An invalid key fails at the signing step, inside the broker, rather
        /// than travelling to a call site as a `String` that fails later.
        #[test]
        fn an_unusable_signing_key_fails_in_the_broker() {
            let Err(error) = github::app_authority_from_key(1, "not a pem") else {
                panic!("an unusable signing key must not yield an authority");
            };
            assert!(error.contains("Invalid private key"), "{error}");
        }

        /// What "remove this installation" has to mean inside the process:
        /// a token minted earlier stops working, not just the stored key.
        #[test]
        fn revoking_an_installation_stops_a_token_already_minted() {
            let installation_id = 5_150;
            let source = CredentialSource::GitHubInstallation { installation_id };
            let audience = LeaseAudience::https("api.github.com");
            let token = crate::security::lease::leases().issue(
                crate::security::lease::LeaseTerms {
                    source: &source,
                    audience: audience.clone(),
                    expires_at: chrono::Utc::now().timestamp() + 3600,
                    purpose: "revocation test",
                },
                "ghs-Rv71Dm35Kp09Ez".to_string(),
            );
            assert!(token.present(&audience).is_ok());

            assert_eq!(github::revoke_installation(installation_id), 1);
            assert!(
                token.present(&audience).is_err(),
                "a token already handed out must stop working when its installation is removed"
            );
        }
    }

    /// Compile-time proof that a brokered credential cannot be formatted,
    /// serialized, or cloned into an un-zeroized copy. Mirrors the probes on
    /// `SecretMaterial`; see `security::secret` for how they work.
    mod no_leaking_impls {
        macro_rules! absence_probe {
            ($module:ident, $target:ty, $bound:path, $test:ident, $message:literal) => {
                mod $module {
                    use std::marker::PhantomData;

                    pub struct Probe<T>(PhantomData<T>);

                    pub trait Absent {
                        fn implements() -> bool {
                            false
                        }
                    }
                    impl<T> Absent for Probe<T> {}

                    impl<T: $bound> Probe<T> {
                        fn implements() -> bool {
                            true
                        }
                    }

                    #[test]
                    fn $test() {
                        assert!(!Probe::<$target>::implements(), $message);
                        assert!(Probe::<String>::implements(), "probe is inert");
                    }
                }
            };
        }

        absence_probe!(
            secret_debug,
            crate::security::broker::BrokeredSecret,
            std::fmt::Debug,
            brokered_secret_has_no_debug,
            "BrokeredSecret must not be formattable"
        );
        absence_probe!(
            secret_serialize,
            crate::security::broker::BrokeredSecret,
            serde::Serialize,
            brokered_secret_has_no_serde,
            "BrokeredSecret must not be serializable"
        );
        absence_probe!(
            secret_clone,
            crate::security::broker::BrokeredSecret,
            Clone,
            brokered_secret_cannot_be_cloned,
            "BrokeredSecret must not be cloneable into an un-zeroized copy"
        );
        absence_probe!(
            config_debug,
            crate::security::broker::BrokeredMcpConfig,
            std::fmt::Debug,
            brokered_config_has_no_debug,
            "an expanded MCP config must not be formattable"
        );
        absence_probe!(
            config_serialize,
            crate::security::broker::BrokeredMcpConfig,
            serde::Serialize,
            brokered_config_has_no_serde,
            "an expanded MCP config must not be serializable into a durable row"
        );
        absence_probe!(
            config_clone,
            crate::security::broker::BrokeredMcpConfig,
            Clone,
            brokered_config_cannot_be_cloned,
            "an expanded MCP config must not be cloneable into a persisted struct"
        );
    }
}
