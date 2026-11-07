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
//! Four places are wired to policy: [`AuthorityPlace::WorkspaceSettings`],
//! [`AuthorityPlace::Tool`] with [`ToolKind::McpServer`] at workspace scope,
//! [`AuthorityPlace::ExternalAccount`] with [`AuthorityAction::Write`], which
//! spending a subscription account's usage-reset credit reaches, and
//! [`AuthorityPlace::ExecutionQueue`], where a thread moves other threads'
//! held work (`Write`) or starts its own past every cap (`Run`). The
//! remaining places and actions exist so the follow-on adapters named in
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
///
/// A new constraint variant does not bump it: the constraint enum is tagged
/// and denies unknown fields, so a build that predates a variant already
/// refuses a grant carrying one. [`AuthorityConstraint::ActingThread`] was
/// added that way. Bump only when an existing encoding changes meaning.
///
/// [`AuthorityConstraint::ExactAct`] did not bump it. What it changes is the
/// meaning of one lifetime, `Once`, so its floor lives in the matcher
/// ([`AuthorityGrant::check`]) rather than here: a version bump would also
/// refuse every standing grant, whose meaning is unchanged.
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
        "ExternalAccount + Run",
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
    /// The workspace's execution queue: the fleet's release order and its
    /// caps, which span every project database. `Write` reorders that line
    /// across threads; `Run` starts held work past every cap.
    ExecutionQueue { workspace_id: String },
    /// How one store bookmark catches up with its base. `Write` sets or clears
    /// its merge catch-up mode, which exempts that one branch from replay and
    /// turns on its landing review gate.
    BranchCatchUp {
        project_id: String,
        bookmark: String,
    },
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
            AuthorityPlace::ExecutionQueue { workspace_id } => {
                format!("workspace/{}/queue", shorthand_segment(workspace_id))
            }
            AuthorityPlace::BranchCatchUp {
                project_id,
                bookmark,
            } => format!(
                "project/{}/branch/{}/catch-up",
                shorthand_segment(project_id),
                shorthand_segment(bookmark)
            ),
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
            AuthorityPlace::ExecutionQueue { .. } => "execution_queue",
            AuthorityPlace::BranchCatchUp { .. } => "branch_catch_up",
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

/// A canonical sha256 encoder for authority identities: every field is written
/// as a length-prefixed key and a length-prefixed value, so no value can
/// impersonate a field boundary and two different inputs cannot produce the
/// same byte stream by splicing. Every digest a grant binds to (an MCP
/// configuration, a catalog binding, an act) is taken through this one
/// encoder, each under its own domain.
pub struct CanonicalEncoder {
    hasher: sha2::Sha256,
}

impl CanonicalEncoder {
    pub fn new(domain: &str, version: u32) -> Self {
        let mut encoder = Self {
            hasher: <sha2::Sha256 as sha2::Digest>::new(),
        };
        encoder.field("domain", domain);
        encoder.field("encoding", &version.to_string());
        encoder
    }

    pub fn field(&mut self, key: &str, value: &str) {
        use sha2::Digest;
        self.hasher.update((key.len() as u64).to_be_bytes());
        self.hasher.update(key.as_bytes());
        self.hasher.update((value.len() as u64).to_be_bytes());
        self.hasher.update(value.as_bytes());
    }

    /// An ordered sequence: the count is hashed too, so a list cannot be
    /// lengthened or shortened without changing the digest.
    pub fn sequence(&mut self, key: &str, values: &[String]) {
        self.field(key, &values.len().to_string());
        for (index, value) in values.iter().enumerate() {
            self.field(&format!("{key}[{index}]"), value);
        }
    }

    /// An unordered map, canonicalized by sorting keys. `HashMap` iteration
    /// order is arbitrary and varies run to run, so without this the same
    /// configuration would fingerprint differently on each attempt and no
    /// standing grant could ever be reused.
    pub fn map(&mut self, key: &str, values: &std::collections::HashMap<String, String>) {
        let sorted: std::collections::BTreeMap<&String, &String> = values.iter().collect();
        self.field(key, &sorted.len().to_string());
        for (name, value) in sorted {
            self.field(&format!("{key}.key"), name);
            self.field(&format!("{key}.value"), value);
        }
    }

    /// A JSON value, canonicalized: every node is tagged with its type before
    /// its content, arrays keep their order and count, and object keys are
    /// sorted. The same value therefore digests the same whatever order its
    /// keys arrived in, while `"1"` and `1`, or `{"a.b":1}` and
    /// `{"a":{"b":1}}`, stay distinct.
    pub fn json(&mut self, key: &str, value: &serde_json::Value) {
        use serde_json::Value;
        let content = format!("{key}.value");
        match value {
            Value::Null => self.field(key, "null"),
            Value::Bool(flag) => {
                self.field(key, "bool");
                self.field(&content, if *flag { "true" } else { "false" });
            }
            Value::Number(number) => {
                self.field(key, "number");
                self.field(&content, &number.to_string());
            }
            Value::String(text) => {
                self.field(key, "string");
                self.field(&content, text);
            }
            Value::Array(items) => {
                self.field(key, "array");
                self.field(&format!("{key}.len"), &items.len().to_string());
                for (index, item) in items.iter().enumerate() {
                    self.json(&format!("{key}[{index}]"), item);
                }
            }
            Value::Object(entries) => {
                self.field(key, "object");
                self.field(&format!("{key}.len"), &entries.len().to_string());
                let sorted: std::collections::BTreeMap<&String, &Value> = entries.iter().collect();
                for (name, item) in sorted {
                    self.field(&format!("{key}.key"), name);
                    self.json(&content, item);
                }
            }
        }
    }

    pub fn finish(self) -> String {
        hex::encode(self.finish_bytes())
    }

    pub fn finish_bytes(self) -> [u8; 32] {
        use sha2::Digest;
        self.hasher.finalize().into()
    }
}

/// Digest algorithm behind [`ActDigest`] and [`ActBinding`].
pub const ACT_DIGEST_ALGORITHM: &str = "sha256";

/// Version of what an [`ActBinding`] is taken over: the canonical encoding an
/// adapter feeds an act, and the salt the binding puts ahead of it. Bump on any
/// change to either, so one binding can never name two acts.
///
/// Version 1 was an unsalted digest of the act. A binding stored at version 1
/// names no act (see [`ActBinding::names`]).
pub const ACT_DIGEST_ENCODING_VERSION: u32 = 2;

/// Bytes of OS randomness each [`ActBinding`] is salted with.
pub const ACT_SALT_BYTES: usize = 32;

const ACT_DIGEST_DOMAIN: &str = "cairn.authority.act";
const ACT_BINDING_DOMAIN: &str = "cairn.authority.act.salted";

/// The identity of the one concrete act a request performs: the resolved
/// content of the change, such as the value written to a settings section or
/// the account a reset is spent from.
///
/// A scope names where an act lands and a mutation mode names its kind; this
/// names the act itself, which is what an operator answering **Once** was
/// shown. Each adapter computes it from the resolved input both check points
/// derive the request from, never from the request's summary or other agent
/// prose, and never from live state that can change between the gate and the
/// pre-persist re-check, since the two must name the same act.
///
/// It is unsalted, so it never leaves memory: it is not serializable, and its
/// `Debug` form shows nothing of it. The content it is taken over can carry a
/// credential, and an unsalted hash of it would let anyone who read it and
/// knew the rest of the content confirm a guessed secret, or tell two
/// identical acts apart from two different ones. What is stored or rendered is
/// an [`ActBinding`].
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ActDigest([u8; 32]);

impl std::fmt::Debug for ActDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ActDigest(..)")
    }
}

impl ActDigest {
    /// Digest the act `kind` names, with `encode` feeding its resolved content.
    /// `kind` separates adapters, so two kinds of act never share a digest even
    /// when their content encodes alike.
    pub fn of(kind: &str, encode: impl FnOnce(&mut CanonicalEncoder)) -> Self {
        let mut encoder = CanonicalEncoder::new(ACT_DIGEST_DOMAIN, ACT_DIGEST_ENCODING_VERSION);
        encoder.field("act", kind);
        encode(&mut encoder);
        Self(encoder.finish_bytes())
    }

    /// The unsalted digest in hex, so a test can assert it is stored nowhere.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn unsalted_hex(&self) -> String {
        hex::encode(self.0)
    }
}

/// An act as a card and a `Once` grant hold it: the [`ActDigest`] taken again
/// under a random salt drawn for that one card.
///
/// The salt is stored beside the digest, and matching recomputes the binding
/// under it, so each grant names its act without naming it the way any other
/// grant does: two grants for the identical act store different digests, and
/// no stored digest can be looked up in a table precomputed over likely acts.
/// A guess at a secret inside the act can still be checked by someone who
/// holds the binding and the rest of the act; closing that would take a keyed
/// digest under a secret the reader cannot reach, and there is no such key to
/// reuse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActBinding {
    pub algorithm: String,
    pub encoding_version: u32,
    /// Hex. Absent on a binding stored before acts were salted, which names no
    /// act; it still parses, so the grant that carries it can be read, listed
    /// and asked about again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub salt: Option<String>,
    pub digest: String,
}

impl ActBinding {
    /// Bind `act` under a fresh salt from the operating system's random number
    /// generator.
    pub fn fresh(act: &ActDigest) -> Self {
        let mut salt = [0u8; ACT_SALT_BYTES];
        getrandom::fill(&mut salt)
            .expect("the operating system's random number generator is available");
        Self::salted(act, &salt)
    }

    fn salted(act: &ActDigest, salt: &[u8]) -> Self {
        Self {
            algorithm: ACT_DIGEST_ALGORITHM.to_string(),
            encoding_version: ACT_DIGEST_ENCODING_VERSION,
            salt: Some(hex::encode(salt)),
            digest: Self::digest(act, salt),
        }
    }

    fn digest(act: &ActDigest, salt: &[u8]) -> String {
        let mut encoder = CanonicalEncoder::new(ACT_BINDING_DOMAIN, ACT_DIGEST_ENCODING_VERSION);
        encoder.field("salt", &hex::encode(salt));
        encoder.field("act", &hex::encode(act.0));
        encoder.finish()
    }

    /// Whether this binding names `act`, recomputed under the binding's own
    /// salt. A binding without a full salt, or from another encoding, names
    /// nothing.
    pub fn names(&self, act: &ActDigest) -> bool {
        if self.algorithm != ACT_DIGEST_ALGORITHM
            || self.encoding_version != ACT_DIGEST_ENCODING_VERSION
        {
            return false;
        }
        let Some(salt) = self.salt.as_deref().and_then(|salt| hex::decode(salt).ok()) else {
            return false;
        };
        salt.len() == ACT_SALT_BYTES && Self::digest(act, &salt) == self.digest
    }

    /// Short display form for a grant listing.
    pub fn short(&self) -> String {
        let head: String = self.digest.chars().take(12).collect();
        format!("{}:v{}:{head}", self.algorithm, self.encoding_version)
    }
}

/// Read whatever a stored request carries for its act, and keep none of it. A
/// prompt stored before acts were salted holds an unsalted digest there, and
/// it must still parse: an authority prompt that fails to parse is no longer
/// recognised as one.
fn discard_stored_act<'de, D>(deserializer: D) -> Result<Option<ActDigest>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    <serde::de::IgnoredAny as Deserialize>::deserialize(deserializer)?;
    Ok(None)
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
    /// The id of the thread the asking run acts for, resolved from the run's
    /// own job (never from anything the run wrote). Set on an execution-queue
    /// request, where a grant is bound to one thread.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acting_thread: Option<String>,
    /// The one concrete act this request performs, which a `Once` grant binds
    /// to. In memory only: see [`ActDigest`]. A stored request reads back
    /// without it, and its card carries the act as an [`ActBinding`] instead.
    #[serde(default, skip_serializing, deserialize_with = "discard_stored_act")]
    pub act: Option<ActDigest>,
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

    /// Attach the thread the asking run acts for.
    pub fn with_acting_thread(mut self, thread_id: impl Into<String>) -> Self {
        self.facts.acting_thread = Some(thread_id.into());
        self
    }

    /// Attach the identity of the concrete act.
    pub fn with_act(mut self, act: ActDigest) -> Self {
        self.facts.act = Some(act);
        self
    }

    /// The act this request performs, bound under a fresh salt, for the card
    /// that asks about it. `None` when the request names no act.
    pub fn bind_act(&self) -> Option<ActBinding> {
        self.facts.act.as_ref().map(ActBinding::fresh)
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

    /// Whether this request acts on the execution queue, and therefore may
    /// only be authorized by a grant bound to the thread asking.
    ///
    /// A thread is not a principal yet: a standing grant otherwise binds to
    /// no one, and "let judgment reorder the fleet" must not become "let any
    /// run reorder the fleet". The [`AuthorityConstraint::ActingThread`]
    /// constraint carries that binding until a thread principal exists.
    pub fn requires_thread_binding(&self) -> bool {
        matches!(self.scope.place, AuthorityPlace::ExecutionQueue { .. })
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
    /// Only a request from a run acting for this thread (by id).
    ///
    /// The operator approves a queue act for the thread named on the card,
    /// so the approval must not answer for another thread that asks for the
    /// same scope, whatever its lifetime.
    ActingThread { thread_id: String },
    /// Only this exact act.
    ///
    /// Attached to `Once` grants only. A card shows one act, and Once answers
    /// "this, this time", so the grant must not be spendable by a different
    /// act in the same scope that happens to arrive first. Turn, session and
    /// standing grants mean "this kind of act" and do not carry it.
    ExactAct { digest: ActBinding },
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
            AuthorityConstraint::ActingThread { thread_id } => {
                if !request.requires_thread_binding() {
                    return true;
                }
                // A request that names no thread matches no thread: an absent
                // fact is never a wildcard.
                request.facts.acting_thread.as_ref() == Some(thread_id)
            }
            // An act is never vacuous: a request that cannot name its act is
            // not the act the operator approved.
            AuthorityConstraint::ExactAct { digest } => request
                .facts
                .act
                .as_ref()
                .is_some_and(|act| digest.names(act)),
        }
    }

    fn is_mcp_config(&self) -> bool {
        matches!(self, AuthorityConstraint::McpConfig { .. })
    }

    fn is_acting_thread(&self) -> bool {
        matches!(self, AuthorityConstraint::ActingThread { .. })
    }

    fn is_exact_act(&self) -> bool {
        matches!(self, AuthorityConstraint::ExactAct { .. })
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

    /// The constraints an operator's approval of `request` earns for
    /// `lifetime`: exactly what the card showed, and no wider.
    ///
    /// - always the mutation mode shown, so approving "reconfigure linear"
    ///   does not also approve removing it;
    /// - for an MCP write, the configuration identity, so the approval covers
    ///   that server and not merely its name;
    /// - for an execution-queue act, the thread that asked, whatever the
    ///   lifetime: a standing approval is "let judgment do this", never "let
    ///   any run do this";
    /// - for `Once`, the act itself, so the single use is spent only by the act
    ///   the card described. `act` is the binding the card was raised with
    ///   ([`AuthorityRequest::bind_act`]), because a stored request no longer
    ///   carries its unsalted act.
    ///
    /// A request missing a fact its approval must bind is an error rather than
    /// a wider grant: minting without the binding would either authorize more
    /// than was shown or, under the structural floors, authorize nothing while
    /// looking valid.
    pub fn for_approval(
        request: &AuthorityRequest,
        act: Option<&ActBinding>,
        lifetime: AuthorityLifetimeKind,
    ) -> Result<Self, String> {
        let mut constraints = vec![AuthorityConstraint::MutationModes {
            modes: vec![request.mutation],
        }];
        if request.requires_mcp_config_binding() {
            let fingerprint = request.facts.mcp_config.clone().ok_or_else(|| {
                "this MCP approval does not name the configuration it would authorize".to_string()
            })?;
            constraints.push(AuthorityConstraint::McpConfig { fingerprint });
        }
        if request.requires_thread_binding() {
            let thread_id = request.facts.acting_thread.clone().ok_or_else(|| {
                "this queue approval does not name the thread it would authorize".to_string()
            })?;
            constraints.push(AuthorityConstraint::ActingThread { thread_id });
        }
        if lifetime == AuthorityLifetimeKind::Once {
            let digest = act.cloned().ok_or_else(|| {
                "this approval does not name the exact act it would authorize once".to_string()
            })?;
            constraints.push(AuthorityConstraint::ExactAct { digest });
        }
        Ok(Self::new(constraints))
    }

    /// The thread an `ActingThread` constraint binds, if the set has one.
    pub fn acting_thread(&self) -> Option<&str> {
        self.constraints
            .iter()
            .find_map(|constraint| match constraint {
                AuthorityConstraint::ActingThread { thread_id } => Some(thread_id.as_str()),
                _ => None,
            })
    }

    /// Whether the set binds one exact act.
    pub fn binds_exact_act(&self) -> bool {
        self.constraints
            .iter()
            .any(AuthorityConstraint::is_exact_act)
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
        // The same floor for the execution queue: a grant that names no
        // thread says nothing about who may move or start work, so it
        // authorizes no queue act at all.
        if request.requires_thread_binding()
            && !self
                .constraints
                .iter()
                .any(AuthorityConstraint::is_acting_thread)
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
/// A `Standing` grant is NOT bound to a run — see [`AuthorityLifetime::Standing`]
/// — but one whose `actor` is set is bound to that actor.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AuthorityPrincipal {
    /// Canonical node URI, when the request came from a node.
    pub node_uri: Option<String>,
    pub run_id: Option<String>,
    pub agent_id: Option<String>,
    /// The verified principal acting, typed. Set for an act that no run makes,
    /// such as a write through a page, where the run fields above are all
    /// `None`. It carries whatever principal the verified appearance names (a
    /// `Human` for a JWT caller, a `Machine` for this install's desktop) and is
    /// never relabeled to fit a role.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<crate::identity::PrincipalRef>,
    /// What the actor acted through, when that is not the actor itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instrument: Option<AuthorityInstrument>,
}

/// The artifact an actor acted through. An instrument is context, not a
/// principal: it holds no authority of its own and is never a delegation hop.
/// "Actor X, through page P@v owned by thread T", or "actor X, through the
/// database browser, in thread T's rows".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthorityInstrument {
    Page {
        /// The canonical page URI.
        page: String,
        version: u32,
        /// The canonical URI of the thread that owns the page and supplies its
        /// authority context. Owning a page is not evidence of authoring a
        /// particular version of it.
        owner: String,
    },
    /// The app's database browser, which the operator used in their own
    /// webview to write rows of a thread's database.
    DbBrowser {
        /// The canonical URI of the thread whose rows were written.
        owner: String,
    },
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
    ///
    /// The one binding a standing grant may carry is its principal's `actor`
    /// (tenet 1334): with `actor = Some(P)` it authorizes only an asking actor
    /// exactly equal to the durable subject P, such as one served-API client;
    /// with `None` it keeps the workspace-wide meaning above.
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
    /// Validated identity that made the decision. Historical grants omit it.
    #[serde(default)]
    pub decision_actor: Option<crate::identity::PrincipalRef>,
    /// Immutable evidence for how the decision actor appeared at decision time.
    #[serde(default)]
    pub appearance_snapshot: Option<crate::identity::AppearanceSnapshot>,
    /// Legacy, untyped approver label retained only for historical decoding.
    #[serde(default)]
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
    /// The typed principal asking, when the request comes from an
    /// authenticated actor rather than a run (a served-API client). Compared
    /// against an actor-bound standing grant; it is carried from the verified
    /// appearance, never inferred from a key, a name or a place.
    pub actor: Option<crate::identity::PrincipalRef>,
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
        // A Once grant authorizes one exact act, structurally: one that does
        // not bind its act (minted before the binding existed, or by a path
        // that forgot it) covers nothing, so the operator is asked again
        // rather than having the first act in scope spend an approval given
        // for a different one. This floor is per lifetime rather than a model
        // version bump, because a version bump would also refuse every
        // standing grant, whose meaning did not change.
        if matches!(self.lifetime, AuthorityLifetime::Once { .. })
            && !self.constraints.binds_exact_act()
        {
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
    /// `Standing` is exempt from the run binding by definition; see
    /// [`AuthorityLifetime::Standing`]. A standing grant bound to an actor
    /// matches only that actor, and a bound actor that is not a valid durable
    /// subject matches nothing.
    fn principal_matches(&self, context: &AuthorityContext) -> bool {
        if matches!(self.lifetime, AuthorityLifetime::Standing) {
            return match &self.principal.actor {
                None => true,
                Some(bound) => {
                    bound
                        .validate_at(crate::identity::PrincipalPosition::DurableSubject)
                        .is_ok()
                        && context.actor.as_ref() == Some(bound)
                }
            };
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
    /// Changing state on an account the operator holds with an outside
    /// provider that Cairn cannot undo, such as spending a usage-reset credit,
    /// which the provider does not refund.
    ExternalAccountWrite,
    /// An outside client of Cairn's served API spending a remote provider's
    /// account. Only a standing grant bound to that client allows it.
    ServedApiRemoteUse,
    /// A thread moving held work across threads in the fleet's release order.
    /// It changes when other threads' work starts, which is beyond what a
    /// thread's own order (its own children, in its own places) may do.
    QueueReorderAcrossThreads,
    /// A thread starting held work past every cap. The caps constrain what a
    /// thread starts itself (tenet 1319), so only the operator may skip them.
    QueueStartNow,
    /// Removing a stored object: its bytes are deleted for every reader and
    /// every address shows a tombstone instead. Destructive, and it decides
    /// over whatever private matter the object shows, so only the operator may
    /// allow it (cairn/6318).
    StoredContentRemoval,
    /// Setting or clearing one branch's merge catch-up mode: that branch is
    /// never replayed onto its base, and its landing is gated on the owner's
    /// review of declared paths. Only the operator may make that exception.
    BranchCatchUpMode,
    /// Trusting a project's library definitions, confirming the authority one
    /// of them carries, or withdrawing either. A trusted project's files grant
    /// tools, hooks and scripts to every agent that resolves them, so only the
    /// operator decides (cairn/6323).
    ProjectLibraryTrust,
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
            AuthorityReason::ExternalAccountWrite => "external_account_write",
            AuthorityReason::ServedApiRemoteUse => "served_api_remote_use",
            AuthorityReason::QueueReorderAcrossThreads => "queue_reorder_across_threads",
            AuthorityReason::QueueStartNow => "queue_start_now",
            AuthorityReason::StoredContentRemoval => "stored_content_removal",
            AuthorityReason::BranchCatchUpMode => "branch_catch_up_mode",
            AuthorityReason::ProjectLibraryTrust => "project_library_trust",
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

    /// The act every fixture request performs, so a fixture Once grant binds
    /// the same act its request names.
    fn fixture_act() -> ActDigest {
        ActDigest::of("fixture", |encoder| encoder.field("act", "the shown act"))
    }

    fn request(scope: AuthorityScope, mutation: AuthorityMutation) -> AuthorityRequest {
        let request =
            AuthorityRequest::new(scope, mutation, "summary".to_string()).with_act(fixture_act());
        if request.requires_mcp_config_binding() {
            request.with_mcp_config(fixture_config())
        } else {
            request
        }
    }

    fn grant(scope: AuthorityScope, lifetime: AuthorityLifetime) -> AuthorityGrant {
        // An MCP grant is always minted with a configuration binding, so a
        // fixture without one would be a grant the system never issues.
        // A Once grant is likewise always minted bound to its act.
        let mut constraints = Vec::new();
        if matches!(
            scope.place,
            AuthorityPlace::Tool {
                kind: ToolKind::McpServer,
                ..
            }
        ) {
            constraints.push(AuthorityConstraint::McpConfig {
                fingerprint: fixture_config(),
            });
        }
        if matches!(lifetime, AuthorityLifetime::Once { .. }) {
            constraints.push(AuthorityConstraint::ExactAct {
                digest: ActBinding::fresh(&fixture_act()),
            });
        }
        let constraints = AuthorityConstraintSet::new(constraints);
        AuthorityGrant {
            id: "grant-1".to_string(),
            scope,
            principal: AuthorityPrincipal {
                node_uri: Some("cairn://p/CAIRN/1/1/builder".to_string()),
                run_id: Some("run-1".to_string()),
                agent_id: Some("build".to_string()),
                ..Default::default()
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
            actor: None,
        }
    }

    fn client(id: &str) -> crate::identity::PrincipalRef {
        crate::identity::PrincipalRef::External {
            provider: "cairn-api".into(),
            namespace: "default".into(),
            id: id.into(),
        }
    }

    #[test]
    fn an_actor_bound_standing_grant_authorizes_only_that_actor() {
        let scope = AuthorityScope::new(
            AuthorityPlace::ExternalAccount {
                provider: "openrouter".into(),
                account_id: "acc_or".into(),
            },
            AuthorityAction::Run,
        );
        let req = request(scope.clone(), AuthorityMutation::Update);
        let mut bound = grant(scope, AuthorityLifetime::Standing);
        bound.principal = AuthorityPrincipal {
            actor: Some(client("cli_dig")),
            ..Default::default()
        };
        let asking = |actor: Option<crate::identity::PrincipalRef>| AuthorityContext {
            run_id: None,
            turn_id: None,
            session_id: None,
            actor,
            ..context()
        };
        assert!(bound.matches(&req, &asking(Some(client("cli_dig"))), 100));
        assert_eq!(
            bound.check(&req, &asking(Some(client("cli_sitrep"))), 100),
            Err(GrantMismatch::Principal)
        );
        assert_eq!(
            bound.check(&req, &asking(None), 100),
            Err(GrantMismatch::Principal)
        );
        // Unbound, it keeps the workspace-wide meaning.
        bound.principal = AuthorityPrincipal::default();
        assert!(bound.matches(&req, &asking(Some(client("cli_sitrep"))), 100));
        assert!(bound.matches(&req, &asking(None), 100));
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
mod exact_act_tests {
    use super::*;

    fn settings(section: &str) -> AuthorityScope {
        AuthorityScope::new(
            AuthorityPlace::WorkspaceSettings {
                workspace_id: "default".to_string(),
                section: section.to_string(),
            },
            AuthorityAction::Write,
        )
    }

    fn value_act(value: serde_json::Value) -> ActDigest {
        ActDigest::of("test.section", |encoder| encoder.json("value", &value))
    }

    fn write(value: serde_json::Value) -> AuthorityRequest {
        AuthorityRequest::new(
            settings("models"),
            AuthorityMutation::Update,
            "change workspace settings section 'models'".to_string(),
        )
        .with_act(value_act(value))
    }

    fn minted(request: &AuthorityRequest, lifetime: AuthorityLifetime) -> AuthorityGrant {
        AuthorityGrant {
            id: "g".to_string(),
            scope: request.scope.clone(),
            principal: AuthorityPrincipal {
                run_id: Some("run-1".to_string()),
                ..Default::default()
            },
            audience: AuthorityAudience::workspace("default"),
            constraints: AuthorityConstraintSet::for_approval(
                request,
                request.bind_act().as_ref(),
                lifetime.kind(),
            )
            .unwrap(),
            lifetime,
            provenance: AuthorityProvenance::default(),
            created_at: 1,
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
            ..Default::default()
        }
    }

    fn once() -> AuthorityLifetime {
        AuthorityLifetime::Once {
            request_id: "perm-1".to_string(),
        }
    }

    #[test]
    fn a_once_grant_covers_the_value_it_was_shown_and_no_other() {
        let shown = write(serde_json::json!({"pools": ["a"]}));
        let grant = minted(&shown, once());
        assert!(grant.matches(&shown, &context(), 2));
        let other = write(serde_json::json!({"pools": ["b"]}));
        assert_eq!(
            grant.check(&other, &context(), 2),
            Err(GrantMismatch::Constraints)
        );
    }

    #[test]
    fn a_request_that_names_no_act_matches_no_once_grant() {
        let shown = write(serde_json::json!(1));
        let grant = minted(&shown, once());
        let mut unnamed = shown.clone();
        unnamed.facts.act = None;
        assert_eq!(
            grant.check(&unnamed, &context(), 2),
            Err(GrantMismatch::Constraints)
        );
    }

    /// The persisted-without-the-constraint case: a Once grant that does not
    /// bind its act covers nothing, and the operator is asked again.
    #[test]
    fn a_once_grant_that_binds_no_act_covers_nothing() {
        let shown = write(serde_json::json!(1));
        let mut legacy = minted(&shown, once());
        legacy.constraints =
            AuthorityConstraintSet::new(vec![AuthorityConstraint::MutationModes {
                modes: vec![AuthorityMutation::Update],
            }]);
        assert_eq!(
            legacy.check(&shown, &context(), 2),
            Err(GrantMismatch::Constraints)
        );
    }

    /// Turn, session and standing grants mean "this kind of act": they bind no
    /// act, and any act in their scope and mode is covered.
    #[test]
    fn wider_lifetimes_cover_every_act_of_their_kind() {
        let shown = write(serde_json::json!(1));
        let other = write(serde_json::json!(2));
        for lifetime in [
            AuthorityLifetime::Turn {
                turn_id: "turn-1".to_string(),
            },
            AuthorityLifetime::Session {
                session_id: "session-1".to_string(),
            },
            AuthorityLifetime::Standing,
        ] {
            let grant = minted(&shown, lifetime.clone());
            assert!(!grant.constraints.binds_exact_act(), "{lifetime:?}");
            assert!(grant.matches(&other, &context(), 2), "{lifetime:?}");
        }
    }

    #[test]
    fn a_once_approval_that_cannot_name_its_act_mints_nothing() {
        let mut unnamed = write(serde_json::json!(1));
        unnamed.facts.act = None;
        assert!(unnamed.bind_act().is_none());
        let error = AuthorityConstraintSet::for_approval(
            &unnamed,
            unnamed.bind_act().as_ref(),
            AuthorityLifetimeKind::Once,
        )
        .unwrap_err();
        assert!(error.contains("exact act"), "{error}");
        // The wider lifetimes never needed one.
        assert!(AuthorityConstraintSet::for_approval(
            &unnamed,
            None,
            AuthorityLifetimeKind::Standing
        )
        .is_ok());
    }

    #[test]
    fn an_exact_act_constraint_round_trips_through_storage() {
        let request = write(serde_json::json!(1));
        let set = AuthorityConstraintSet::for_approval(
            &request,
            request.bind_act().as_ref(),
            AuthorityLifetimeKind::Once,
        )
        .unwrap();
        let json = serde_json::to_string(&set).unwrap();
        assert!(json.contains("\"constraint\":\"exact_act\""), "{json}");
        assert!(json.contains("\"salt\""), "{json}");
        let parsed = AuthorityConstraintSet::parse(&json).unwrap();
        assert_eq!(parsed, set);
        assert!(parsed
            .constraints
            .iter()
            .all(|constraint| constraint.covers(&request)));
    }

    /// Each binding draws its own salt, so two Once grants for the identical
    /// act store different digests, and each still names that act.
    #[test]
    fn two_bindings_of_one_act_differ_and_each_names_it() {
        let shown = write(serde_json::json!({"api_key": "sk-dummy"}));
        let first = minted(&shown, once());
        let second = minted(&shown, once());
        let binding = |grant: &AuthorityGrant| match &grant.constraints.constraints[..] {
            [.., AuthorityConstraint::ExactAct { digest }] => digest.clone(),
            other => panic!("no act binding: {other:?}"),
        };
        let (a, b) = (binding(&first), binding(&second));
        assert_ne!(a.salt, b.salt);
        assert_ne!(a.digest, b.digest);
        assert_eq!(a.salt.as_deref().map(str::len), Some(ACT_SALT_BYTES * 2));
        for grant in [&first, &second] {
            assert!(grant.matches(&shown, &context(), 2));
        }
        // Neither stores the act's unsalted digest.
        let act = shown.facts.act.as_ref().unwrap();
        for stored in [&a, &b] {
            assert_ne!(stored.digest, act.unsalted_hex());
        }
    }

    /// A binding moved onto another salt, or onto another act's digest, names
    /// nothing.
    #[test]
    fn a_binding_names_only_its_own_act_under_its_own_salt() {
        let shown = write(serde_json::json!(1));
        let act = shown.facts.act.clone().unwrap();
        let binding = ActBinding::fresh(&act);
        assert!(binding.names(&act));
        assert!(!binding.names(&value_act(serde_json::json!(2))));
        let mut resalted = binding.clone();
        resalted.salt = ActBinding::fresh(&act).salt;
        assert!(!resalted.names(&act));
        let mut short_salt = binding.clone();
        short_salt.salt = Some("00".to_string());
        assert!(!short_salt.names(&act));
    }

    /// The shape 5999 stored: an unsalted version-1 digest. It still parses,
    /// so the grant carrying it can be read and listed, and it covers nothing.
    #[test]
    fn an_unsalted_binding_stored_before_salting_names_no_act() {
        let shown = write(serde_json::json!(1));
        let stored = r#"{"version":3,"constraints":[
            {"constraint":"mutation_modes","modes":["update"]},
            {"constraint":"exact_act","digest":{"algorithm":"sha256","encoding_version":1,"digest":"abc"}}
        ]}"#
            .replace("\"version\":3", &format!("\"version\":{AUTHORITY_MODEL_VERSION}"));
        let mut legacy = minted(&shown, once());
        legacy.constraints = AuthorityConstraintSet::parse(&stored).unwrap();
        assert!(legacy.constraints.binds_exact_act());
        assert_eq!(
            legacy.check(&shown, &context(), 2),
            Err(GrantMismatch::Constraints)
        );
    }

    /// Every binding without a full salt authorizes nothing, the act it was
    /// minted for included: one the scrub migration blanked, an empty one, and
    /// even one carrying that act's own unsalted digest. An empty or missing
    /// digest never compares equal to anything.
    #[test]
    fn a_scrubbed_or_unsalted_binding_authorizes_not_even_its_own_act() {
        let shown = write(serde_json::json!({"api_key": "sk-dummy"}));
        let act = shown.facts.act.clone().unwrap();
        let salt = ActBinding::fresh(&act).salt;
        let mut cases = Vec::new();
        for encoding_version in [1, ACT_DIGEST_ENCODING_VERSION] {
            for digest in [String::new(), act.unsalted_hex()] {
                for salt in [None, Some(String::new())] {
                    cases.push(ActBinding {
                        algorithm: ACT_DIGEST_ALGORITHM.to_string(),
                        encoding_version,
                        salt,
                        digest: digest.clone(),
                    });
                }
            }
        }
        // A full salt with the digest blanked away is no better.
        cases.push(ActBinding {
            algorithm: ACT_DIGEST_ALGORITHM.to_string(),
            encoding_version: ACT_DIGEST_ENCODING_VERSION,
            salt,
            digest: String::new(),
        });
        for binding in cases {
            assert!(!binding.names(&act), "{binding:?}");
            let mut grant = minted(&shown, once());
            grant.constraints = AuthorityConstraintSet::new(vec![
                AuthorityConstraint::MutationModes {
                    modes: vec![AuthorityMutation::Update],
                },
                AuthorityConstraint::ExactAct {
                    digest: binding.clone(),
                },
            ]);
            let stored = serde_json::to_string(&grant.constraints).unwrap();
            grant.constraints = AuthorityConstraintSet::parse(&stored).unwrap();
            assert_eq!(
                grant.check(&shown, &context(), 2),
                Err(GrantMismatch::Constraints),
                "{binding:?}"
            );
        }
    }

    /// A request never serializes its unsalted act, and a request stored with
    /// one before salting still parses, without it.
    #[test]
    fn a_stored_request_carries_no_unsalted_act() {
        let shown = write(serde_json::json!({"api_key": "sk-dummy"}));
        let json = serde_json::to_string(&shown).unwrap();
        let act = shown.facts.act.as_ref().unwrap();
        assert!(!json.contains(&act.unsalted_hex()), "{json}");
        assert!(!json.contains("\"act\""), "{json}");
        assert!(!format!("{shown:?}").contains(&act.unsalted_hex()));

        let mut stored: serde_json::Value = serde_json::from_str(&json).unwrap();
        stored["facts"]["act"] = serde_json::json!({
            "algorithm": "sha256",
            "encoding_version": 1,
            "digest": act.unsalted_hex(),
        });
        let parsed: AuthorityRequest = serde_json::from_value(stored).unwrap();
        assert_eq!(parsed.facts.act, None);
        assert_eq!(parsed.scope, shown.scope);
    }

    #[test]
    fn the_canonical_json_digest_ignores_key_order_and_nothing_else() {
        let a = value_act(serde_json::json!({"x": 1, "y": [true, null]}));
        let b = value_act(serde_json::from_str(r#"{"y":[true,null],"x":1}"#).unwrap());
        assert_eq!(a, b);
        for different in [
            serde_json::json!({"x": "1", "y": [true, null]}),
            serde_json::json!({"x": 1, "y": [null, true]}),
            serde_json::json!({"x": 1, "y": [true]}),
            serde_json::json!({"x": 1}),
        ] {
            assert_ne!(value_act(different.clone()), a, "{different}");
        }
        assert_ne!(
            value_act(serde_json::json!({"a.b": 1})),
            value_act(serde_json::json!({"a": {"b": 1}}))
        );
        // Two kinds of act never share a digest over the same content.
        assert_ne!(
            ActDigest::of("one", |encoder| encoder
                .json("value", &serde_json::json!(1))),
            ActDigest::of("two", |encoder| encoder
                .json("value", &serde_json::json!(1)))
        );
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
    fn a_thread_constraint_says_nothing_about_an_mcp_write() {
        let constraints = AuthorityConstraintSet::new(vec![
            AuthorityConstraint::MutationModes {
                modes: vec![AuthorityMutation::Create],
            },
            AuthorityConstraint::McpConfig {
                fingerprint: fingerprint("config-a"),
            },
            AuthorityConstraint::ActingThread {
                thread_id: "t-judgment".to_string(),
            },
        ]);
        assert!(constraints.covers(&mcp_write(AuthorityMutation::Create, Some("config-a"))));
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

#[cfg(test)]
mod thread_constraint_tests {
    use super::*;

    fn queue(action: AuthorityAction, thread: Option<&str>) -> AuthorityRequest {
        let request = AuthorityRequest::new(
            AuthorityScope::new(
                AuthorityPlace::ExecutionQueue {
                    workspace_id: "default".to_string(),
                },
                action,
            ),
            AuthorityMutation::Update,
            "move cairn/5901 to fleet 1st".to_string(),
        );
        match thread {
            Some(thread) => request.with_acting_thread(thread),
            None => request,
        }
    }

    fn bound(thread: &str) -> AuthorityConstraintSet {
        AuthorityConstraintSet::new(vec![
            AuthorityConstraint::MutationModes {
                modes: vec![AuthorityMutation::Update],
            },
            AuthorityConstraint::ActingThread {
                thread_id: thread.to_string(),
            },
        ])
    }

    #[test]
    fn the_queue_place_has_its_own_shorthand_and_kind() {
        let scope = queue(AuthorityAction::Write, None).scope;
        assert_eq!(scope.shorthand(), "workspace/default/queue:write");
        assert_eq!(scope.place_kind(), "execution_queue");
        assert_eq!(
            queue(AuthorityAction::Run, None).scope.shorthand(),
            "workspace/default/queue:run"
        );
    }

    #[test]
    fn the_grant_bound_to_a_thread_covers_that_threads_request() {
        for action in [AuthorityAction::Write, AuthorityAction::Run] {
            assert!(bound("t-judgment").covers(&queue(action, Some("t-judgment"))));
        }
    }

    #[test]
    fn an_unbound_grant_never_covers_a_queue_request() {
        // The structural floor: a grant that names no thread, minted by a
        // path that forgot the binding, authorizes nothing here.
        let request = queue(AuthorityAction::Write, Some("t-judgment"));
        assert!(!AuthorityConstraintSet::default().covers(&request));
        let mode_only = AuthorityConstraintSet::new(vec![AuthorityConstraint::MutationModes {
            modes: vec![AuthorityMutation::Update],
        }]);
        assert!(!mode_only.covers(&request));
    }

    #[test]
    fn another_threads_id_does_not_match() {
        assert!(!bound("t-judgment").covers(&queue(AuthorityAction::Write, Some("t-ui"))));
        assert!(!bound("t-judgment").covers(&queue(AuthorityAction::Run, Some("t-ui"))));
    }

    #[test]
    fn a_request_that_names_no_thread_matches_nothing() {
        assert!(!bound("t-judgment").covers(&queue(AuthorityAction::Write, None)));
    }

    #[test]
    fn the_thread_constraint_is_vacuous_on_other_places() {
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
        assert!(!settings.requires_thread_binding());
        assert!(bound("t-judgment").covers(&settings));
    }

    #[test]
    fn the_thread_constraint_and_fact_round_trip() {
        let constraints = bound("t-judgment");
        let json = serde_json::to_string(&constraints).unwrap();
        assert!(json.contains("\"constraint\":\"acting_thread\""), "{json}");
        assert_eq!(AuthorityConstraintSet::parse(&json).unwrap(), constraints);

        let request = queue(AuthorityAction::Run, Some("t-judgment"));
        let json = serde_json::to_string(&request).unwrap();
        let back: AuthorityRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back, request);
        // A request without the fact keeps today's encoding.
        let settings_facts = serde_json::to_value(AuthorityFacts::default()).unwrap();
        assert_eq!(settings_facts, serde_json::json!({}));
    }

    #[test]
    fn the_queue_reasons_have_stable_codes() {
        assert_eq!(
            AuthorityReason::QueueReorderAcrossThreads.as_str(),
            "queue_reorder_across_threads"
        );
        assert_eq!(AuthorityReason::QueueStartNow.as_str(), "queue_start_now");
        assert_eq!(
            AuthorityReason::StoredContentRemoval.as_str(),
            "stored_content_removal"
        );
        assert_eq!(
            AuthorityReason::BranchCatchUpMode.as_str(),
            "branch_catch_up_mode"
        );
    }
}
