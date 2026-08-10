//! Canonical authority scopes and journaled grants.
//!
//! One structural vocabulary underpins every authorization decision in Cairn:
//!
//! ```text
//! scope    = place + action
//! grant    = scope + principal + audience + lifetime + constraints + provenance
//! decision = policy(scope, ownership/context) + matching active grant
//! ```
//!
//! A scope says only **where** and **what operation**. It deliberately does not
//! say why an operation is dangerous, who may perform it, how long an approval
//! lasts, or what prompted the approval — those are policy and grant concerns,
//! and folding them into the scope name is what turns an authorization model
//! into an ad hoc IAM language. `Tool(ws, McpServer, "linear") + Write` is the
//! same scope whether it was approved once or standing, by whom, and why.
//!
//! # Layering
//!
//! Authorization is **not** runtime containment. The logical namespace fence
//! (`cairn_core::mcp::handlers::fence`) owns concrete containment: a kernel
//! sandbox denial, a read of a sensitive host path, a write that escapes the
//! project namespace. Its `Once`/`Session` answers are containment exceptions
//! for a concrete path, not authority grants, and its `PermissionScope` is a
//! different concept from [`AuthorityLifetime`] here. A grant must never
//! pre-disable containment.
//!
//! # What v1 enforces
//!
//! Only two places are wired to policy: [`AuthorityPlace::WorkspaceSettings`]
//! and [`AuthorityPlace::Tool`] with [`ToolKind::McpServer`] at workspace
//! scope. The remaining places exist so the follow-on adapters named in
//! [`FOLLOW_ON_ADAPTERS`] normalize into this vocabulary rather than inventing
//! parallel names later; they carry no enforcement today.

use serde::{Deserialize, Serialize};

/// Version of the persisted grant encoding (scope, constraints, audience,
/// lifetime). A stored grant carrying any other version is refused rather than
/// best-effort parsed: an authorization record we cannot interpret exactly must
/// never be treated as an approval.
///
/// v2 added [`AuthorityConstraint::McpConfig`] and made it **required** for an
/// MCP tool mutation. Refusing v1 on load is the point rather than a cost: a v1
/// grant names a server without naming what runs under that name, so honouring
/// one would let a stale approval authorize an arbitrary command.
pub const AUTHORITY_MODEL_VERSION: u32 = 2;

/// Adapters that will consume this model, with the failure mode each catches.
/// Recorded here so the next agent extends the vocabulary instead of inventing
/// a second one. None of these are enforced in v1.
pub const FOLLOW_ON_ADAPTERS: &[(&str, &str)] = &[
    (
        "Executor + Enroll/Revoke/Write",
        "unapproved durable machine trust or identity rebinding",
    ),
    (
        "executor/project or credential audience expansion",
        "a trusted machine gains a broader data or host domain; placement and routine Run stay direct",
    ),
    (
        "ExternalAccount + Write/Run",
        "operator identity misuse",
    ),
    (
        "Resource + Write (irreversible, outside actor ownership)",
        "unrecoverable cross-boundary loss",
    ),
    (
        "HostPath + Read/Write",
        "containment escape; stays under the fence until a separately reviewed migration",
    ),
];

// ============================================================================
// Scope: place + action
// ============================================================================

/// The kind of tool a [`AuthorityPlace::Tool`] names. Closed on purpose: a new
/// tool kind is a deliberate vocabulary addition, not a free-form string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolKind {
    McpServer,
}

impl ToolKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ToolKind::McpServer => "mcp",
        }
    }
}

/// Where authority is exercised.
///
/// Every identity in a place derives from an actual resolved target — a
/// canonical settings section key, a normalized MCP server name, a resolved
/// device id — never from display text or an agent-authored string. Two
/// requests that touch the same thing must normalize to the same place, and two
/// requests that touch different things must not.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "place", rename_all = "snake_case")]
pub enum AuthorityPlace {
    /// The workspace as a whole.
    Workspace { workspace_id: String },
    /// One canonical section of the workspace settings document. The section is
    /// the settings key itself (`backends`, `keybinds`, …), so the place is as
    /// narrow as the thing actually being written.
    WorkspaceSettings {
        workspace_id: String,
        section: String,
    },
    /// One project.
    Project { project_id: String },
    /// One enrolled executor, identified by the runner that holds its
    /// enrollment plus the machine it names. Not wired in v1.
    Executor {
        runner_device_id: String,
        executor_id: String,
        device_id: String,
    },
    /// One configured tool in a workspace, by kind and normalized name.
    Tool {
        workspace_id: String,
        kind: ToolKind,
        canonical_name: String,
    },
    /// An account with an external identity provider. Not wired in v1.
    ExternalAccount {
        provider: String,
        account_id: String,
    },
    /// A concrete host path. Reserved for a future fence migration; the fence
    /// owns containment today. Not wired in v1.
    HostPath { canonical_path: String },
    /// A canonical `cairn://` resource. Not wired in v1.
    Resource { canonical_uri: String },
}

/// What operation is performed at a place.
///
/// Truthful operations only. There is deliberately no `ManageCapabilities`,
/// `ActAsIdentity`, `ExpandReach`, or `DestroyCrossBoundary`: those name why an
/// operation matters, which is policy's job, and encoding them here would make
/// the same physical write carry different scope names depending on how someone
/// felt about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityAction {
    Read,
    Write,
    Run,
    /// Reserved for the executor adapter. Not produced in v1.
    Enroll,
    /// Reserved for the executor adapter. Not produced in v1.
    Revoke,
}

impl AuthorityAction {
    pub fn as_str(self) -> &'static str {
        match self {
            AuthorityAction::Read => "read",
            AuthorityAction::Write => "write",
            AuthorityAction::Run => "run",
            AuthorityAction::Enroll => "enroll",
            AuthorityAction::Revoke => "revoke",
        }
    }
}

/// A normalized authority scope: exactly a place and an action.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuthorityScope {
    pub place: AuthorityPlace,
    pub action: AuthorityAction,
}

/// Escape the `/` and `:` that structure a shorthand so two different places can
/// never render to the same string. Without this, a settings section literally
/// named `mcp/linear` would collide with a tool place.
fn shorthand_segment(raw: &str) -> String {
    raw.replace('%', "%25")
        .replace('/', "%2F")
        .replace(':', "%3A")
}

impl AuthorityScope {
    pub fn new(place: AuthorityPlace, action: AuthorityAction) -> Self {
        Self { place, action }
    }

    /// Human-readable shorthand, e.g. `workspace/default/tool/mcp/linear:write`.
    ///
    /// Derived from the structured form, never parsed back into one: security
    /// decisions use [`AuthorityScope`] itself. The shorthand is injective (see
    /// [`shorthand_segment`]) so it is safe to use as a storage index key that a
    /// structural comparison then confirms.
    pub fn shorthand(&self) -> String {
        let place = match &self.place {
            AuthorityPlace::Workspace { workspace_id } => {
                format!("workspace/{}", shorthand_segment(workspace_id))
            }
            AuthorityPlace::WorkspaceSettings {
                workspace_id,
                section,
            } => format!(
                "workspace/{}/settings/{}",
                shorthand_segment(workspace_id),
                shorthand_segment(section)
            ),
            AuthorityPlace::Project { project_id } => {
                format!("project/{}", shorthand_segment(project_id))
            }
            AuthorityPlace::Executor {
                runner_device_id,
                executor_id,
                device_id,
            } => format!(
                "executor/{}/{}/{}",
                shorthand_segment(runner_device_id),
                shorthand_segment(executor_id),
                shorthand_segment(device_id)
            ),
            AuthorityPlace::Tool {
                workspace_id,
                kind,
                canonical_name,
            } => format!(
                "workspace/{}/tool/{}/{}",
                shorthand_segment(workspace_id),
                kind.as_str(),
                shorthand_segment(canonical_name)
            ),
            AuthorityPlace::ExternalAccount {
                provider,
                account_id,
            } => format!(
                "account/{}/{}",
                shorthand_segment(provider),
                shorthand_segment(account_id)
            ),
            AuthorityPlace::HostPath { canonical_path } => {
                format!("host/{}", shorthand_segment(canonical_path))
            }
            AuthorityPlace::Resource { canonical_uri } => {
                format!("resource/{}", shorthand_segment(canonical_uri))
            }
        };
        format!("{place}:{}", self.action.as_str())
    }

    /// Stable discriminant for the place, stored alongside the scope so a query
    /// can filter by family without parsing JSON.
    pub fn place_kind(&self) -> &'static str {
        match &self.place {
            AuthorityPlace::Workspace { .. } => "workspace",
            AuthorityPlace::WorkspaceSettings { .. } => "workspace_settings",
            AuthorityPlace::Project { .. } => "project",
            AuthorityPlace::Executor { .. } => "executor",
            AuthorityPlace::Tool { .. } => "tool",
            AuthorityPlace::ExternalAccount { .. } => "external_account",
            AuthorityPlace::HostPath { .. } => "host_path",
            AuthorityPlace::Resource { .. } => "resource",
        }
    }
}

// ============================================================================
// The concrete mutation a request performs
// ============================================================================

/// The shape of the transition a `Write` performs at its place.
///
/// A scope is intentionally coarse (`Tool(…) + Write` covers install, edit, and
/// removal alike) so that the vocabulary stays small. The mutation mode is what
/// a constraint narrows against, so an operator can approve reconfiguring a
/// server without also approving deleting it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityMutation {
    Create,
    Update,
    Delete,
}

impl AuthorityMutation {
    pub fn as_str(self) -> &'static str {
        match self {
            AuthorityMutation::Create => "create",
            AuthorityMutation::Update => "update",
            AuthorityMutation::Delete => "delete",
        }
    }
}

/// The identity of a **resultant** MCP server configuration: what would
/// actually be registered and, later, executed or connected to.
///
/// A scope names the registry entry (`tool/mcp/linear`); this names what will
/// live at it. The two are deliberately separate — the place is still the place
/// whatever is configured there — and keeping configuration identity in a
/// constraint is what lets a standing approval mean "this exact server" rather
/// than "anything anyone ever registers under this name".
///
/// Only the algorithm, the encoding version, and the digest are carried. The
/// digest is taken over the configuration **as authored**, so a `${TOKEN}`
/// reference is hashed as the literal string `${TOKEN}`: changing which secret
/// a server reads changes the identity, while no secret value is ever read,
/// expanded, stored, or rendered to produce one.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpConfigFingerprint {
    /// Digest algorithm, e.g. `sha256`.
    pub algorithm: String,
    /// Version of the canonical encoding fed to the digest. Bumped whenever the
    /// field set or its ordering changes, so two builds can never disagree
    /// about what a digest covers while producing the same string.
    pub encoding_version: u32,
    /// Lowercase hex digest.
    pub digest: String,
}

impl McpConfigFingerprint {
    /// Short display form for a prompt or a grant listing.
    pub fn short(&self) -> String {
        let head: String = self.digest.chars().take(12).collect();
        format!("{}:v{}:{head}", self.algorithm, self.encoding_version)
    }
}

/// Typed, system-derived facts about the concrete change a request performs.
///
/// These come from the resolved bytes or configuration that would actually be
/// persisted, never from agent-authored text, and they are what a grant's
/// constraints are matched against. A fact absent here cannot satisfy a
/// constraint that requires it, which is what makes a missing fingerprint a
/// refusal rather than a wildcard.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthorityFacts {
    /// The resultant MCP server configuration this request would leave behind
    /// (for a delete, the entry it would remove).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mcp_config: Option<McpConfigFingerprint>,
}

/// A normalized authorization request: the resolved scope plus the concrete
/// mutation and the typed facts derived from it, ready for policy
/// classification and grant matching.
///
/// `summary` is system-rendered from the resolved target for the approval
/// prompt. It is descriptive only — it never participates in matching, so no
/// agent-authored prose can widen what a grant covers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityRequest {
    pub scope: AuthorityScope,
    pub mutation: AuthorityMutation,
    pub summary: String,
    #[serde(default)]
    pub facts: AuthorityFacts,
}

impl AuthorityRequest {
    pub fn new(scope: AuthorityScope, mutation: AuthorityMutation, summary: String) -> Self {
        Self {
            scope,
            mutation,
            summary,
            facts: AuthorityFacts::default(),
        }
    }

    /// Attach the resultant MCP configuration identity.
    pub fn with_mcp_config(mut self, fingerprint: McpConfigFingerprint) -> Self {
        self.facts.mcp_config = Some(fingerprint);
        self
    }

    /// Whether this request writes an MCP server entry, and therefore may only
    /// be authorized by a grant bound to a configuration identity.
    ///
    /// Read/Run at a tool place are excluded: invoking an already-configured
    /// server exercises existing authority and must never be gated on matching
    /// the config it was approved with.
    pub fn requires_mcp_config_binding(&self) -> bool {
        matches!(
            (&self.scope.place, self.scope.action),
            (
                AuthorityPlace::Tool {
                    kind: ToolKind::McpServer,
                    ..
                },
                AuthorityAction::Write
            )
        )
    }
}

// ============================================================================
// Constraints
// ============================================================================

/// A typed narrowing attached to a grant.
///
/// Constraints only ever narrow: a grant with no constraints covers its whole
/// scope, and adding one can only reduce what matches. There is deliberately no
/// constraint that widens place coverage, no wildcard, and no predicate
/// language — a grant that could grow its own reach is not auditable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "constraint", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthorityConstraint {
    /// Only these exact settings sections.
    SettingsSections { sections: Vec<String> },
    /// Only these exact mutation modes.
    MutationModes { modes: Vec<AuthorityMutation> },
    /// Only this exact resultant MCP server configuration.
    ///
    /// Bound to the configuration the operator was actually shown, so reusing
    /// an approval requires reproducing what it approved. Changing the command,
    /// transport, URL, arguments, environment or header wiring, OAuth
    /// configuration, enabled state, scope, or server name all change the
    /// digest and re-prompt.
    McpConfig { fingerprint: McpConfigFingerprint },
}

impl AuthorityConstraint {
    /// Whether this constraint permits `request`. A constraint that does not
    /// apply to the request's place is vacuously satisfied; narrowing happens
    /// where the constraint is meaningful.
    pub fn covers(&self, request: &AuthorityRequest) -> bool {
        match self {
            AuthorityConstraint::SettingsSections { sections } => match &request.scope.place {
                AuthorityPlace::WorkspaceSettings { section, .. } => sections.contains(section),
                _ => true,
            },
            AuthorityConstraint::MutationModes { modes } => modes.contains(&request.mutation),
            AuthorityConstraint::McpConfig { fingerprint } => {
                if !request.requires_mcp_config_binding() {
                    return true;
                }
                // A request that could not name what it would configure does
                // not match a constraint that names one. Treating an absent
                // fingerprint as "anything" is precisely the substitution this
                // constraint exists to prevent.
                request.facts.mcp_config.as_ref() == Some(fingerprint)
            }
        }
    }

    fn is_mcp_config(&self) -> bool {
        matches!(self, AuthorityConstraint::McpConfig { .. })
    }
}

/// Versioned envelope for a grant's constraint set.
///
/// The version is checked on load, not coerced: a grant persisted by a future
/// encoding is refused, because silently ignoring a constraint we do not
/// understand would widen an approval the operator actually narrowed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityConstraintSet {
    pub version: u32,
    pub constraints: Vec<AuthorityConstraint>,
}

impl Default for AuthorityConstraintSet {
    fn default() -> Self {
        Self {
            version: AUTHORITY_MODEL_VERSION,
            constraints: Vec::new(),
        }
    }
}

impl AuthorityConstraintSet {
    pub fn new(constraints: Vec<AuthorityConstraint>) -> Self {
        Self {
            version: AUTHORITY_MODEL_VERSION,
            constraints,
        }
    }

    /// Parse a persisted constraint set, refusing an unknown encoding version.
    pub fn parse(json: &str) -> Result<Self, String> {
        let set: AuthorityConstraintSet = serde_json::from_str(json)
            .map_err(|e| format!("unreadable authority constraints: {e}"))?;
        if set.version != AUTHORITY_MODEL_VERSION {
            return Err(format!(
                "authority constraint version {} is not understood by this build (expected {AUTHORITY_MODEL_VERSION})",
                set.version
            ));
        }
        Ok(set)
    }

    pub fn covers(&self, request: &AuthorityRequest) -> bool {
        // An MCP write may only be authorized by a grant that binds the
        // configuration identity. This is a structural floor rather than one
        // more constraint to satisfy: an unconstrained grant — whether minted
        // before this rule existed or by a future code path that forgot to
        // attach the constraint — must not authorize an MCP mutation at all,
        // because "approved for tool/mcp/linear" says nothing about what runs.
        if request.requires_mcp_config_binding()
            && !self
                .constraints
                .iter()
                .any(AuthorityConstraint::is_mcp_config)
        {
            return false;
        }
        self.constraints
            .iter()
            .all(|constraint| constraint.covers(request))
    }
}

// ============================================================================
// Principal, audience, lifetime, provenance
// ============================================================================

/// Who exercised the authority.
///
/// For an anchored lifetime (`Once`, `Turn`, `Session`) the principal is part of
/// the binding: the grant authorizes the run it was minted for and no other.
/// Without that, a single-use approval the operator granted to one run could be
/// consumed by a different run that happened to ask for the same scope
/// concurrently, and the journal would record an allow attributed to a principal
/// the operator never saw.
///
/// A `Standing` grant is deliberately NOT principal-bound — see
/// [`AuthorityLifetime::Standing`].
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AuthorityPrincipal {
    /// Canonical node URI, when the request came from a node.
    pub node_uri: Option<String>,
    pub run_id: Option<String>,
    pub agent_id: Option<String>,
}

/// The context a grant is bound to. Contextual binding is separate from the
/// scope name on purpose: the same scope means the same thing everywhere, and
/// the audience is what says *where this particular approval applies*.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityAudience {
    pub workspace_id: String,
    pub project_id: Option<String>,
    pub team_id: Option<String>,
    /// Resolved executor/device, for the future executor adapter.
    pub executor_id: Option<String>,
    pub device_id: Option<String>,
}

impl AuthorityAudience {
    pub fn workspace(workspace_id: impl Into<String>) -> Self {
        Self {
            workspace_id: workspace_id.into(),
            project_id: None,
            team_id: None,
            executor_id: None,
            device_id: None,
        }
    }
}

/// How long a grant is good for, with the anchor that defines "still current".
///
/// Distinct from the fence's `PermissionScope`, which is a containment
/// exception for a concrete host path held in process memory. An
/// [`AuthorityLifetime`] is journaled, survives a runner restart, and is
/// revocable.
///
/// The anchor is also the binding: `Once`, `Turn`, and `Session` each belong to
/// exactly one run, so they cannot leak to another principal. `Standing` is
/// deliberately unanchored — that is what standing means, and it is why it is
/// listable and revocable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "lifetime", rename_all = "snake_case")]
pub enum AuthorityLifetime {
    /// One authorization, consumed atomically with the decision that cites it.
    Once { request_id: String },
    /// Every matching authorization within one canonical turn.
    Turn { turn_id: String },
    /// Every matching authorization within one durable job session.
    Session { session_id: String },
    /// Until it expires or is revoked.
    ///
    /// Standing authority is **workspace-wide on this install**: it is not bound
    /// to the run, node, agent, or project that requested it, and any later run
    /// asking for the same scope is authorized by it. That breadth is the point
    /// — an operator choosing "always" is answering "stop asking me about this",
    /// not "allow this one agent" — and it is why standing is the only lifetime
    /// that is listed and revocable rather than expiring on its own.
    Standing,
}

impl AuthorityLifetime {
    pub fn kind(&self) -> AuthorityLifetimeKind {
        match self {
            AuthorityLifetime::Once { .. } => AuthorityLifetimeKind::Once,
            AuthorityLifetime::Turn { .. } => AuthorityLifetimeKind::Turn,
            AuthorityLifetime::Session { .. } => AuthorityLifetimeKind::Session,
            AuthorityLifetime::Standing => AuthorityLifetimeKind::Standing,
        }
    }

    /// The anchor id, or `None` for `Standing`.
    pub fn anchor(&self) -> Option<&str> {
        match self {
            AuthorityLifetime::Once { request_id } => Some(request_id),
            AuthorityLifetime::Turn { turn_id } => Some(turn_id),
            AuthorityLifetime::Session { session_id } => Some(session_id),
            AuthorityLifetime::Standing => None,
        }
    }
}

/// The lifetime an operator picked, before the anchor is resolved from context.
/// This is what crosses the API/UI boundary; the service turns it into an
/// anchored [`AuthorityLifetime`], so a caller can never choose its own anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityLifetimeKind {
    Once,
    Turn,
    Session,
    Standing,
}

impl AuthorityLifetimeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            AuthorityLifetimeKind::Once => "once",
            AuthorityLifetimeKind::Turn => "turn",
            AuthorityLifetimeKind::Session => "session",
            AuthorityLifetimeKind::Standing => "standing",
        }
    }

    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "once" => Ok(AuthorityLifetimeKind::Once),
            "turn" => Ok(AuthorityLifetimeKind::Turn),
            "session" => Ok(AuthorityLifetimeKind::Session),
            "standing" => Ok(AuthorityLifetimeKind::Standing),
            other => Err(format!(
                "unknown authority lifetime '{other}'; expected once|turn|session|standing"
            )),
        }
    }
}

/// Where a grant came from and who is answerable for it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AuthorityProvenance {
    /// What minted the grant, e.g. `"operator_prompt"`.
    pub issuer: String,
    /// The operator who approved it, when one is identified.
    pub approver: Option<String>,
    /// The permission request / proposal that prompted the approval.
    pub request_uri: Option<String>,
    /// The issue or node the approval happened under.
    pub node_uri: Option<String>,
    pub rationale: Option<String>,
}

// ============================================================================
// Grant
// ============================================================================

/// A journaled binding of a scope to a principal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityGrant {
    pub id: String,
    pub scope: AuthorityScope,
    pub principal: AuthorityPrincipal,
    pub audience: AuthorityAudience,
    pub lifetime: AuthorityLifetime,
    pub constraints: AuthorityConstraintSet,
    pub provenance: AuthorityProvenance,
    pub created_at: i64,
    pub expires_at: Option<i64>,
    pub consumed_at: Option<i64>,
    pub revoked_at: Option<i64>,
}

/// The live context an authorization happens in: which audience the request
/// resolved to, and which lifetime anchors are currently in force.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AuthorityContext {
    pub audience: Option<AuthorityAudience>,
    /// The run asking. Compared against an anchored grant's principal, so a
    /// single-use or turn/session approval cannot be spent by another run.
    pub run_id: Option<String>,
    pub turn_id: Option<String>,
    pub session_id: Option<String>,
    /// The permission request being adjudicated, if this authorization is the
    /// redispatch of an answered prompt.
    pub request_id: Option<String>,
}

/// Why a grant did not match. Returned rather than a bare `bool` so a denial is
/// diagnosable from the journal instead of requiring a debugger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantMismatch {
    Scope,
    Audience,
    Principal,
    Constraints,
    Expired,
    Revoked,
    Consumed,
    AnchorNotCurrent,
}

impl GrantMismatch {
    pub fn as_str(self) -> &'static str {
        match self {
            GrantMismatch::Scope => "scope",
            GrantMismatch::Audience => "audience",
            GrantMismatch::Principal => "principal",
            GrantMismatch::Constraints => "constraints",
            GrantMismatch::Expired => "expired",
            GrantMismatch::Revoked => "revoked",
            GrantMismatch::Consumed => "consumed",
            GrantMismatch::AnchorNotCurrent => "anchor_not_current",
        }
    }
}

impl AuthorityGrant {
    /// Whether this grant authorizes `request` in `context` at time `now`.
    ///
    /// Every condition is an equality or a currency check — there is no partial
    /// credit and no nearest match. A grant either covers exactly what is being
    /// asked, in exactly the context it was issued for, or it does not apply.
    pub fn check(
        &self,
        request: &AuthorityRequest,
        context: &AuthorityContext,
        now: i64,
    ) -> Result<(), GrantMismatch> {
        if self.scope != request.scope {
            return Err(GrantMismatch::Scope);
        }
        if context.audience.as_ref() != Some(&self.audience) {
            return Err(GrantMismatch::Audience);
        }
        if !self.principal_matches(context) {
            return Err(GrantMismatch::Principal);
        }
        if !self.constraints.covers(request) {
            return Err(GrantMismatch::Constraints);
        }
        if self.revoked_at.is_some() {
            return Err(GrantMismatch::Revoked);
        }
        if self.consumed_at.is_some() {
            return Err(GrantMismatch::Consumed);
        }
        if self.expires_at.is_some_and(|expiry| expiry <= now) {
            return Err(GrantMismatch::Expired);
        }
        if !self.anchor_is_current(context) {
            return Err(GrantMismatch::AnchorNotCurrent);
        }
        Ok(())
    }

    pub fn matches(
        &self,
        request: &AuthorityRequest,
        context: &AuthorityContext,
        now: i64,
    ) -> bool {
        self.check(request, context, now).is_ok()
    }

    /// Whether this grant belongs to the run now asking.
    ///
    /// An anchored grant authorizes exactly the run it was minted for. `Once` in
    /// particular relies on this rather than on its anchor: its anchor is the
    /// answered request, which stays "current" until consumption, so without a
    /// principal check two runs racing for the same scope could have one
    /// consume the other's single-use approval.
    ///
    /// `Standing` is exempt by definition; see [`AuthorityLifetime::Standing`].
    fn principal_matches(&self, context: &AuthorityContext) -> bool {
        if matches!(self.lifetime, AuthorityLifetime::Standing) {
            return true;
        }
        match (self.principal.run_id.as_deref(), context.run_id.as_deref()) {
            (Some(granted), Some(asking)) => granted == asking,
            // A grant minted without a run cannot be shown to belong to the run
            // asking now, and an asking run we cannot identify cannot be shown
            // to own the grant. Both fail closed.
            _ => false,
        }
    }

    /// Whether the grant's lifetime anchor is still the live one.
    ///
    /// A `Once` grant's anchor is the permission request that minted it, so it
    /// is current whenever it has not been consumed — the redispatch that
    /// consumes it may legitimately arrive with no request id in context (a
    /// fresh turn re-driving the verb). Consumption, checked separately and
    /// atomically, is what makes it single-use; [`Self::principal_matches`] is
    /// what keeps it bound to one run.
    fn anchor_is_current(&self, context: &AuthorityContext) -> bool {
        match &self.lifetime {
            AuthorityLifetime::Once { .. } => true,
            AuthorityLifetime::Turn { turn_id } => context.turn_id.as_deref() == Some(turn_id),
            AuthorityLifetime::Session { session_id } => {
                context.session_id.as_deref() == Some(session_id)
            }
            AuthorityLifetime::Standing => true,
        }
    }

    /// True once the grant can never authorize anything again, so a listing can
    /// show it as history rather than as live authority.
    pub fn is_spent(&self, now: i64) -> bool {
        self.revoked_at.is_some()
            || self.consumed_at.is_some()
            || self.expires_at.is_some_and(|expiry| expiry <= now)
    }

    /// One-word status for listings.
    pub fn status(&self, now: i64) -> &'static str {
        if self.revoked_at.is_some() {
            "revoked"
        } else if self.consumed_at.is_some() {
            "consumed"
        } else if self.expires_at.is_some_and(|expiry| expiry <= now) {
            "expired"
        } else {
            "active"
        }
    }
}

// ============================================================================
// Policy
// ============================================================================

/// Why policy classified a request the way it did. Stable codes: they are
/// journaled and rendered, and a downstream consumer keys off them.
///
/// A reason is not part of the scope. Changing why something needs approval
/// must never change the identity of what is being approved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityReason {
    /// A workspace settings section that changes tools, agent defaults, checks,
    /// setup/terminal commands, or permission settings for every future agent.
    WorkspaceSettingsCapability,
    /// A settings section this build has not classified. Fails closed: a new
    /// authority-bearing section must not become directly writable merely
    /// because nobody remembered to list it.
    UnclassifiedWorkspaceSettingsSection,
    /// Installing, removing, enabling, or reconfiguring a workspace MCP server
    /// wires executable or network capability for every future agent.
    WorkspaceToolCapability,
    /// Within the actor's established authority; no blast radius expansion.
    WithinExistingAuthority,
    /// Structurally invalid or an invariant violation. Approval cannot legalize
    /// it.
    StructurallyInvalid,
}

impl AuthorityReason {
    pub fn as_str(self) -> &'static str {
        match self {
            AuthorityReason::WorkspaceSettingsCapability => "workspace_settings_capability",
            AuthorityReason::UnclassifiedWorkspaceSettingsSection => {
                "unclassified_workspace_settings_section"
            }
            AuthorityReason::WorkspaceToolCapability => "workspace_tool_capability",
            AuthorityReason::WithinExistingAuthority => "within_existing_authority",
            AuthorityReason::StructurallyInvalid => "structurally_invalid",
        }
    }
}

/// How policy classifies a normalized request, before any grant is consulted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityPolicy {
    /// Proceed. No grant lookup, no journal entry — ordinary work must stay
    /// free of authorization bookkeeping.
    Direct,
    /// A named authority boundary. An active matching grant authorizes it;
    /// otherwise the operator is asked.
    RequiresApproval(AuthorityReason),
    /// Never allowed. A grant cannot override this.
    Forbidden(AuthorityReason),
}

impl AuthorityPolicy {
    pub fn reason(&self) -> Option<AuthorityReason> {
        match self {
            AuthorityPolicy::Direct => None,
            AuthorityPolicy::RequiresApproval(reason) | AuthorityPolicy::Forbidden(reason) => {
                Some(*reason)
            }
        }
    }
}

/// The outcome of an authorization check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityDecision {
    /// Policy said this is ordinary work.
    Direct,
    /// An approval boundary, satisfied by this grant id.
    AllowedByGrant {
        grant_id: String,
        reason: AuthorityReason,
    },
    /// An approval boundary with no matching active grant.
    ApprovalRequired(AuthorityReason),
    /// Structurally refused.
    Forbidden(AuthorityReason),
}

impl AuthorityDecision {
    /// Whether the mutation may proceed.
    pub fn is_allowed(&self) -> bool {
        matches!(
            self,
            AuthorityDecision::Direct | AuthorityDecision::AllowedByGrant { .. }
        )
    }

    /// Stable outcome tag for the authorization journal.
    pub fn as_str(&self) -> &'static str {
        match self {
            AuthorityDecision::Direct => "direct",
            AuthorityDecision::AllowedByGrant { .. } => "allowed_by_grant",
            AuthorityDecision::ApprovalRequired(_) => "approval_required",
            AuthorityDecision::Forbidden(_) => "forbidden",
        }
    }

    pub fn reason(&self) -> Option<AuthorityReason> {
        match self {
            AuthorityDecision::Direct => None,
            AuthorityDecision::AllowedByGrant { reason, .. }
            | AuthorityDecision::ApprovalRequired(reason)
            | AuthorityDecision::Forbidden(reason) => Some(*reason),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws_settings(section: &str) -> AuthorityScope {
        AuthorityScope::new(
            AuthorityPlace::WorkspaceSettings {
                workspace_id: "default".to_string(),
                section: section.to_string(),
            },
            AuthorityAction::Write,
        )
    }

    fn mcp_tool(name: &str) -> AuthorityScope {
        AuthorityScope::new(
            AuthorityPlace::Tool {
                workspace_id: "default".to_string(),
                kind: ToolKind::McpServer,
                canonical_name: name.to_string(),
            },
            AuthorityAction::Write,
        )
    }

    /// The identity every fixture MCP request carries. These tests are about
    /// lifetimes, anchors, principals, and expiry; binding the configuration
    /// consistently on both sides keeps the structural MCP floor satisfied so
    /// it does not mask what they are actually asserting.
    fn fixture_config() -> McpConfigFingerprint {
        McpConfigFingerprint {
            algorithm: "sha256".to_string(),
            encoding_version: 1,
            digest: "fixture-config".to_string(),
        }
    }

    fn request(scope: AuthorityScope, mutation: AuthorityMutation) -> AuthorityRequest {
        let request = AuthorityRequest::new(scope, mutation, "summary".to_string());
        if request.requires_mcp_config_binding() {
            request.with_mcp_config(fixture_config())
        } else {
            request
        }
    }

    fn grant(scope: AuthorityScope, lifetime: AuthorityLifetime) -> AuthorityGrant {
        // An MCP grant is always minted with a configuration binding, so a
        // fixture without one would be a grant the system never issues.
        let constraints = if matches!(
            scope.place,
            AuthorityPlace::Tool {
                kind: ToolKind::McpServer,
                ..
            }
        ) {
            AuthorityConstraintSet::new(vec![AuthorityConstraint::McpConfig {
                fingerprint: fixture_config(),
            }])
        } else {
            AuthorityConstraintSet::default()
        };
        AuthorityGrant {
            id: "grant-1".to_string(),
            scope,
            principal: AuthorityPrincipal {
                node_uri: Some("cairn://p/CAIRN/1/1/builder".to_string()),
                run_id: Some("run-1".to_string()),
                agent_id: Some("build".to_string()),
            },
            audience: AuthorityAudience::workspace("default"),
            lifetime,
            constraints,
            provenance: AuthorityProvenance {
                issuer: "operator_prompt".to_string(),
                ..Default::default()
            },
            created_at: 100,
            expires_at: None,
            consumed_at: None,
            revoked_at: None,
        }
    }

    fn context() -> AuthorityContext {
        AuthorityContext {
            audience: Some(AuthorityAudience::workspace("default")),
            run_id: Some("run-1".to_string()),
            turn_id: Some("turn-1".to_string()),
            session_id: Some("session-1".to_string()),
            request_id: None,
        }
    }

    // ── Scope identity ──────────────────────────────────────────────────────

    #[test]
    fn scope_shorthand_names_only_place_and_action() {
        assert_eq!(
            mcp_tool("linear").shorthand(),
            "workspace/default/tool/mcp/linear:write"
        );
        assert_eq!(
            ws_settings("backends").shorthand(),
            "workspace/default/settings/backends:write"
        );
    }

    #[test]
    fn scope_identity_is_independent_of_lifetime_principal_audience_and_reason() {
        // The same scope must survive every non-scope thing changing around it,
        // or a grant issued under one prompt would not match the next request
        // for the identical operation.
        let scope = mcp_tool("linear");
        let once = grant(
            scope.clone(),
            AuthorityLifetime::Once {
                request_id: "r".into(),
            },
        );
        let mut standing = grant(scope.clone(), AuthorityLifetime::Standing);
        standing.principal = AuthorityPrincipal::default();
        standing.audience = AuthorityAudience::workspace("other");
        standing.constraints =
            AuthorityConstraintSet::new(vec![AuthorityConstraint::MutationModes {
                modes: vec![AuthorityMutation::Delete],
            }]);
        assert_eq!(once.scope, standing.scope);
        assert_eq!(once.scope.shorthand(), standing.scope.shorthand());
    }

    #[test]
    fn shorthand_escapes_structural_delimiters_so_places_cannot_collide() {
        // A settings section literally named `mcp/linear` must not render to the
        // same shorthand as the linear MCP tool place.
        let sneaky = ws_settings("mcp/linear");
        assert_ne!(sneaky.shorthand(), mcp_tool("linear").shorthand());
        assert!(sneaky.shorthand().contains("%2F"));
    }

    #[test]
    fn scope_round_trips_through_json() {
        for scope in [ws_settings("backends"), mcp_tool("linear")] {
            let json = serde_json::to_string(&scope).unwrap();
            let back: AuthorityScope = serde_json::from_str(&json).unwrap();
            assert_eq!(scope, back);
        }
    }

    #[test]
    fn malformed_place_action_and_lifetime_are_rejected() {
        assert!(serde_json::from_str::<AuthorityPlace>(r#"{"place":"nowhere"}"#).is_err());
        assert!(serde_json::from_str::<AuthorityAction>(r#""manage_capabilities""#).is_err());
        assert!(serde_json::from_str::<AuthorityLifetime>(r#"{"lifetime":"forever"}"#).is_err());
        assert!(serde_json::from_str::<ToolKind>(r#""shell""#).is_err());
        assert!(AuthorityLifetimeKind::parse("eternal").is_err());
    }

    // ── Constraints ─────────────────────────────────────────────────────────

    #[test]
    fn constraint_set_refuses_unknown_encoding_version() {
        let good = serde_json::to_string(&AuthorityConstraintSet::default()).unwrap();
        assert!(AuthorityConstraintSet::parse(&good).is_ok());

        let future = r#"{"version":99,"constraints":[]}"#;
        let error = AuthorityConstraintSet::parse(future).unwrap_err();
        assert!(error.contains("not understood"), "{error}");

        let unknown_variant = format!(
            r#"{{"version":{AUTHORITY_MODEL_VERSION},"constraints":[{{"constraint":"anything_goes"}}]}}"#
        );
        assert!(AuthorityConstraintSet::parse(&unknown_variant).is_err());
    }

    #[test]
    fn constraints_narrow_sections_and_mutation_modes() {
        let set = AuthorityConstraintSet::new(vec![
            AuthorityConstraint::SettingsSections {
                sections: vec!["backends".to_string()],
            },
            AuthorityConstraint::MutationModes {
                modes: vec![AuthorityMutation::Update],
            },
        ]);
        assert!(set.covers(&request(ws_settings("backends"), AuthorityMutation::Update)));
        assert!(!set.covers(&request(ws_settings("keybinds"), AuthorityMutation::Update)));
        assert!(!set.covers(&request(ws_settings("backends"), AuthorityMutation::Delete)));
    }

    // ── Matching ────────────────────────────────────────────────────────────

    #[test]
    fn standing_grant_matches_its_exact_scope() {
        let g = grant(mcp_tool("linear"), AuthorityLifetime::Standing);
        assert!(g.matches(
            &request(mcp_tool("linear"), AuthorityMutation::Update),
            &context(),
            200
        ));
    }

    #[test]
    fn match_fails_on_wrong_tool_action_audience_or_constraint() {
        let base = grant(mcp_tool("linear"), AuthorityLifetime::Standing);
        let ctx = context();

        // Different tool.
        assert_eq!(
            base.check(
                &request(mcp_tool("github"), AuthorityMutation::Update),
                &ctx,
                200
            ),
            Err(GrantMismatch::Scope)
        );
        // Different action at the same place.
        let read_scope = AuthorityScope::new(mcp_tool("linear").place, AuthorityAction::Run);
        assert_eq!(
            base.check(&request(read_scope, AuthorityMutation::Update), &ctx, 200),
            Err(GrantMismatch::Scope)
        );
        // Different workspace place.
        let other_ws = AuthorityScope::new(
            AuthorityPlace::Tool {
                workspace_id: "other".to_string(),
                kind: ToolKind::McpServer,
                canonical_name: "linear".to_string(),
            },
            AuthorityAction::Write,
        );
        assert_eq!(
            base.check(&request(other_ws, AuthorityMutation::Update), &ctx, 200),
            Err(GrantMismatch::Scope)
        );
        // Different audience.
        let mut foreign = ctx.clone();
        foreign.audience = Some(AuthorityAudience::workspace("other"));
        assert_eq!(
            base.check(
                &request(mcp_tool("linear"), AuthorityMutation::Update),
                &foreign,
                200
            ),
            Err(GrantMismatch::Audience)
        );
        // Narrowed away by a constraint.
        let mut narrowed = base.clone();
        narrowed.constraints =
            AuthorityConstraintSet::new(vec![AuthorityConstraint::MutationModes {
                modes: vec![AuthorityMutation::Update],
            }]);
        assert_eq!(
            narrowed.check(
                &request(mcp_tool("linear"), AuthorityMutation::Delete),
                &ctx,
                200
            ),
            Err(GrantMismatch::Constraints)
        );
    }

    #[test]
    fn expiry_revocation_and_consumption_block_reuse() {
        let req = request(mcp_tool("linear"), AuthorityMutation::Update);
        let ctx = context();

        let mut expired = grant(mcp_tool("linear"), AuthorityLifetime::Standing);
        expired.expires_at = Some(150);
        assert_eq!(expired.check(&req, &ctx, 200), Err(GrantMismatch::Expired));
        // Still live a moment before expiry.
        assert!(expired.matches(&req, &ctx, 100));

        let mut revoked = grant(mcp_tool("linear"), AuthorityLifetime::Standing);
        revoked.revoked_at = Some(150);
        assert_eq!(revoked.check(&req, &ctx, 200), Err(GrantMismatch::Revoked));

        let mut consumed = grant(
            mcp_tool("linear"),
            AuthorityLifetime::Once {
                request_id: "perm-1".to_string(),
            },
        );
        consumed.consumed_at = Some(150);
        assert_eq!(
            consumed.check(&req, &ctx, 200),
            Err(GrantMismatch::Consumed)
        );
    }

    #[test]
    fn turn_and_session_grants_expire_with_their_anchor() {
        let req = request(mcp_tool("linear"), AuthorityMutation::Update);

        let turn = grant(
            mcp_tool("linear"),
            AuthorityLifetime::Turn {
                turn_id: "turn-1".to_string(),
            },
        );
        assert!(turn.matches(&req, &context(), 200));
        let mut next_turn = context();
        next_turn.turn_id = Some("turn-2".to_string());
        assert_eq!(
            turn.check(&req, &next_turn, 200),
            Err(GrantMismatch::AnchorNotCurrent)
        );

        let session = grant(
            mcp_tool("linear"),
            AuthorityLifetime::Session {
                session_id: "session-1".to_string(),
            },
        );
        // A session grant survives the turn advancing — that is the whole
        // difference between the two lifetimes.
        assert!(session.matches(&req, &next_turn, 200));
        let mut next_session = context();
        next_session.session_id = Some("session-2".to_string());
        assert_eq!(
            session.check(&req, &next_session, 200),
            Err(GrantMismatch::AnchorNotCurrent)
        );
    }

    #[test]
    fn an_anchored_grant_belongs_to_the_run_it_was_minted_for() {
        let req = request(mcp_tool("linear"), AuthorityMutation::Update);
        let once = grant(
            mcp_tool("linear"),
            AuthorityLifetime::Once {
                request_id: "perm-1".to_string(),
            },
        );
        assert!(once.matches(&req, &context(), 200));

        // Another run racing for the same scope must not be able to spend this
        // run's single-use approval.
        let mut other_run = context();
        other_run.run_id = Some("run-2".to_string());
        assert_eq!(
            once.check(&req, &other_run, 200),
            Err(GrantMismatch::Principal)
        );

        // The same holds for turn and session grants.
        for lifetime in [
            AuthorityLifetime::Turn {
                turn_id: "turn-1".to_string(),
            },
            AuthorityLifetime::Session {
                session_id: "session-1".to_string(),
            },
        ] {
            assert_eq!(
                grant(mcp_tool("linear"), lifetime).check(&req, &other_run, 200),
                Err(GrantMismatch::Principal)
            );
        }
    }

    #[test]
    fn an_anchored_grant_fails_closed_when_either_side_has_no_run() {
        let req = request(mcp_tool("linear"), AuthorityMutation::Update);
        let mut anonymous = grant(
            mcp_tool("linear"),
            AuthorityLifetime::Once {
                request_id: "perm-1".to_string(),
            },
        );
        anonymous.principal = AuthorityPrincipal::default();
        assert_eq!(
            anonymous.check(&req, &context(), 200),
            Err(GrantMismatch::Principal)
        );

        let bound = grant(
            mcp_tool("linear"),
            AuthorityLifetime::Once {
                request_id: "perm-1".to_string(),
            },
        );
        let mut unidentified = context();
        unidentified.run_id = None;
        assert_eq!(
            bound.check(&req, &unidentified, 200),
            Err(GrantMismatch::Principal)
        );
    }

    #[test]
    fn standing_authority_is_workspace_wide_by_design() {
        // Standing means "stop asking me about this scope", not "allow this one
        // run", so it deliberately authorizes a different run. This is the
        // breadth that makes it the only lifetime worth listing and revoking.
        let req = request(mcp_tool("linear"), AuthorityMutation::Update);
        let standing = grant(mcp_tool("linear"), AuthorityLifetime::Standing);
        let mut other_run = context();
        other_run.run_id = Some("run-2".to_string());
        assert!(standing.matches(&req, &other_run, 200));
    }

    #[test]
    fn status_reports_the_first_reason_a_grant_is_spent() {
        let mut g = grant(mcp_tool("linear"), AuthorityLifetime::Standing);
        assert_eq!(g.status(200), "active");
        assert!(!g.is_spent(200));
        g.expires_at = Some(150);
        assert_eq!(g.status(200), "expired");
        g.consumed_at = Some(160);
        assert_eq!(g.status(200), "consumed");
        g.revoked_at = Some(170);
        assert_eq!(g.status(200), "revoked");
        assert!(g.is_spent(200));
    }

    #[test]
    fn decision_allows_only_direct_and_granted() {
        assert!(AuthorityDecision::Direct.is_allowed());
        assert!(AuthorityDecision::AllowedByGrant {
            grant_id: "g".to_string(),
            reason: AuthorityReason::WorkspaceToolCapability,
        }
        .is_allowed());
        assert!(
            !AuthorityDecision::ApprovalRequired(AuthorityReason::WorkspaceToolCapability)
                .is_allowed()
        );
        assert!(!AuthorityDecision::Forbidden(AuthorityReason::StructurallyInvalid).is_allowed());
    }
}

#[cfg(test)]
mod mcp_constraint_tests {
    use super::*;

    fn fingerprint(digest: &str) -> McpConfigFingerprint {
        McpConfigFingerprint {
            algorithm: "sha256".to_string(),
            encoding_version: 1,
            digest: digest.to_string(),
        }
    }

    fn mcp_write(mutation: AuthorityMutation, digest: Option<&str>) -> AuthorityRequest {
        let request = AuthorityRequest::new(
            AuthorityScope::new(
                AuthorityPlace::Tool {
                    workspace_id: "default".to_string(),
                    kind: ToolKind::McpServer,
                    canonical_name: "linear".to_string(),
                },
                AuthorityAction::Write,
            ),
            mutation,
            "install workspace MCP server 'linear'".to_string(),
        );
        match digest {
            Some(digest) => request.with_mcp_config(fingerprint(digest)),
            None => request,
        }
    }

    fn bound(digest: &str, mutation: AuthorityMutation) -> AuthorityConstraintSet {
        AuthorityConstraintSet::new(vec![
            AuthorityConstraint::MutationModes {
                modes: vec![mutation],
            },
            AuthorityConstraint::McpConfig {
                fingerprint: fingerprint(digest),
            },
        ])
    }

    #[test]
    fn the_same_resultant_configuration_matches() {
        let constraints = bound("config-a", AuthorityMutation::Create);
        assert!(constraints.covers(&mcp_write(AuthorityMutation::Create, Some("config-a"))));
    }

    #[test]
    fn a_different_configuration_under_the_same_name_does_not() {
        // This is the whole point of the constraint: reusing a server name must
        // not reuse the approval for whatever used to run under it.
        let constraints = bound("config-a", AuthorityMutation::Create);
        assert!(!constraints.covers(&mcp_write(AuthorityMutation::Create, Some("config-b"))));
    }

    #[test]
    fn a_request_that_names_no_configuration_matches_nothing() {
        // An absent fingerprint is not a wildcard. A request that could not say
        // what it would configure must not be able to spend an approval that
        // could.
        let constraints = bound("config-a", AuthorityMutation::Create);
        assert!(!constraints.covers(&mcp_write(AuthorityMutation::Create, None)));
    }

    #[test]
    fn an_unconstrained_grant_never_authorizes_an_mcp_write() {
        // The structural floor. A grant carrying no configuration binding --
        // minted before this rule existed, or by a path that forgot to attach
        // one -- says nothing about what would run, so it authorizes nothing.
        let unconstrained = AuthorityConstraintSet::default();
        assert!(!unconstrained.covers(&mcp_write(AuthorityMutation::Create, Some("config-a"))));

        let mutation_only = AuthorityConstraintSet::new(vec![AuthorityConstraint::MutationModes {
            modes: vec![AuthorityMutation::Create],
        }]);
        assert!(!mutation_only.covers(&mcp_write(AuthorityMutation::Create, Some("config-a"))));
    }

    #[test]
    fn the_mutation_stays_isolated_from_the_configuration() {
        // Matching the config is necessary, not sufficient: approving an
        // install of exactly this server is still not approving its removal.
        let constraints = bound("config-a", AuthorityMutation::Create);
        assert!(!constraints.covers(&mcp_write(AuthorityMutation::Delete, Some("config-a"))));
    }

    #[test]
    fn a_configuration_constraint_does_not_leak_onto_other_places() {
        // Constraints only narrow where they are meaningful. A settings write
        // carries no MCP configuration and must not be refused for lacking one.
        let settings = AuthorityRequest::new(
            AuthorityScope::new(
                AuthorityPlace::WorkspaceSettings {
                    workspace_id: "default".to_string(),
                    section: "backends".to_string(),
                },
                AuthorityAction::Write,
            ),
            AuthorityMutation::Update,
            "change workspace settings section 'backends'".to_string(),
        );
        assert!(bound("config-a", AuthorityMutation::Update).covers(&settings));
    }

    #[test]
    fn invoking_a_configured_server_needs_no_configuration_binding() {
        // Running an already-configured tool exercises existing authority. If
        // it required matching the config it was approved with, every ordinary
        // tool call would start failing the moment the server was edited.
        let run = AuthorityRequest::new(
            AuthorityScope::new(
                AuthorityPlace::Tool {
                    workspace_id: "default".to_string(),
                    kind: ToolKind::McpServer,
                    canonical_name: "linear".to_string(),
                },
                AuthorityAction::Run,
            ),
            AuthorityMutation::Update,
            "invoke linear".to_string(),
        );
        assert!(!run.requires_mcp_config_binding());
        assert!(AuthorityConstraintSet::default().covers(&run));
    }

    #[test]
    fn a_constraint_encoding_this_build_does_not_understand_is_refused() {
        let future = serde_json::json!({"version": AUTHORITY_MODEL_VERSION + 1, "constraints": []})
            .to_string();
        let error = AuthorityConstraintSet::parse(&future).unwrap_err();
        assert!(error.contains("not understood"), "{error}");

        // Including the encoding this change replaced: a v1 grant named a
        // server without naming what runs under it, so honouring one would let
        // a stale approval authorize an arbitrary command.
        let legacy = serde_json::json!({"version": 1, "constraints": []}).to_string();
        assert!(AuthorityConstraintSet::parse(&legacy).is_err());
    }

    #[test]
    fn a_fingerprint_round_trips_and_shows_only_its_digest() {
        let constraints = bound("deadbeefcafebabe0123", AuthorityMutation::Create);
        let json = serde_json::to_string(&constraints).unwrap();
        assert_eq!(AuthorityConstraintSet::parse(&json).unwrap(), constraints);
        assert_eq!(
            fingerprint("deadbeefcafebabe0123").short(),
            "sha256:v1:deadbeefcafe"
        );
    }
}
