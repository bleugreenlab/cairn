//! Self-routing entity ids: format primitives, the route parser, the typed id
//! newtypes, and the mint helpers (CAIRN-2210).
//!
//! Cairn stores each entity WHOLLY in one database: the private (per-install) DB
//! for everything local, or a team-owned synced replica for a team project's
//! collaboration data. Routing a write to the right database used to mean probing
//! every open database for the row. This module makes the id self-routing instead:
//! a ProjectScoped entity owned by a team carries that team's id as a prefix, so
//! resolving its owning database is a prefix parse plus an O(1) open-DB-map lookup
//! — no probe.
//!
//! ## Format
//!
//! - **Team-owned id**: `{team_id}~{uuid-v4}` — the team id, a `~` separator, and
//!   a canonical lowercase v4 UUID.
//! - **Local id**: a bare canonical v4 UUID (and, for a few legacy/caller-supplied
//!   ids like project keys and todo positions, any bare non-uuid string). There is
//!   NO reserved local marker: *bare already means local*, which is exactly why no
//!   existing row needs migrating — every id minted before teams existed is bare,
//!   and bare routes Local.
//!
//! ## Two distinct domains — do not conflate
//!
//! [`RouteScope`] (here) answers *where does this id route* — Local or a specific
//! Team. `TableScope` (in `cairn-core`'s migrations module) answers *how is this
//! TABLE classified* — Private / ProjectScoped / SharedContent. They are
//! independent: a local issue lives in a ProjectScoped table but has a bare
//! `RouteScope::Local` id. Collapsing the two is a bug-magnet.
//!
//! ## Routing is not authorization
//!
//! The prefix is a routing HINT a client could forge. What stops cross-team access
//! is the authenticating sync broker plus auth checks against the account/team
//! context — never this parser. `parse_route_scope` must not be read as an access
//! check.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Where an id routes. The routing domain (distinct from a table's `TableScope`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RouteScope {
    /// The private/per-install database. Every bare id routes here.
    Local,
    /// The synced replica owned by this team. The string is the team id (the
    /// opaque org PK, NOT the renameable slug).
    Team(String),
}

/// A malformed *prefixed* id. Bare ids never produce an `IdError` — they are
/// deliberately not uuid-validated so legacy/caller-supplied local ids (project
/// keys, todo positions) keep routing Local. Strictness is reserved for the
/// prefixed form, where forgery or corruption actually matters and where minting
/// is under our control.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdError {
    /// A `~` is present but not at the one canonical position (`len - 37`), so the
    /// id cannot be the `{team}~{uuid}` form.
    Malformed,
    /// The prefix before `~` fails the team-id guard (empty, too long, or carries
    /// a character outside `[A-Za-z0-9]`).
    BadTeamPrefix,
    /// The suffix after `~` is not a canonical lowercase v4 UUID.
    BadUuidSuffix,
}

impl std::fmt::Display for IdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IdError::Malformed => {
                write!(f, "id has a '~' outside the canonical team-prefix position")
            }
            IdError::BadTeamPrefix => write!(f, "id's team prefix fails the team-id guard"),
            IdError::BadUuidSuffix => write!(f, "id's suffix is not a canonical lowercase v4 UUID"),
        }
    }
}

impl std::error::Error for IdError {}

/// The number of trailing bytes a canonical v4 UUID occupies (`8-4-4-4-12` plus 4
/// hyphens). A team-prefixed id therefore carries `~` at byte `len - 37`.
const UUID_LEN: usize = 36;
const SUFFIX_WITH_SEP_LEN: usize = UUID_LEN + 1;

/// The maximum accepted team-id length. The live org PK is 32 chars; this leaves
/// generous headroom while bounding the prefix so a pathological id cannot grow
/// without limit.
const MAX_TEAM_ID_LEN: usize = 64;

/// The team-id guard: non-empty, bounded, and `[A-Za-z0-9]` only. Applied both at
/// team registration (reject loudly) and in the parser. The opaque better-auth org
/// PK (e.g. `lElEfCpn0UbajBELSHhVEGkO66Hext4o`) satisfies it; the renameable slug
/// is never used as the routing prefix.
pub fn is_valid_team_id(team_id: &str) -> bool {
    !team_id.is_empty()
        && team_id.len() <= MAX_TEAM_ID_LEN
        && team_id.bytes().all(|b| b.is_ascii_alphanumeric())
}

/// Is `s` a canonical lowercase v4 UUID (`xxxxxxxx-xxxx-4xxx-{8,9,a,b}xxx-xxxxxxxxxxxx`)?
/// This is what `Uuid::new_v4().to_string()` emits; the parser holds the prefixed
/// form to exactly this shape so a corrupted or forged suffix is rejected.
fn is_canonical_v4_uuid(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != UUID_LEN {
        return false;
    }
    for (i, &c) in b.iter().enumerate() {
        let ok = match i {
            8 | 13 | 18 | 23 => c == b'-',
            14 => c == b'4',                                       // version nibble
            19 => matches!(c, b'8' | b'9' | b'a' | b'b'),          // variant nibble
            _ => c.is_ascii_digit() || (b'a'..=b'f').contains(&c), // lowercase hex
        };
        if !ok {
            return false;
        }
    }
    true
}

/// Parse an id's routing scope.
///
/// - No `~` ⇒ [`RouteScope::Local`] (bare passthrough — NOT uuid-strict, so a
///   legacy/caller-supplied local id such as a project key or todo position still
///   routes Local).
/// - `~` at byte `len - 37`, a guard-passing prefix, and a canonical lowercase v4
///   suffix ⇒ [`RouteScope::Team`].
/// - Any `~`-bearing id that fails those prefixed rules ⇒ `Err` (a forged or
///   corrupted prefixed id must never silently route Local).
pub fn parse_route_scope(id: &str) -> Result<RouteScope, IdError> {
    if !id.contains('~') {
        return Ok(RouteScope::Local);
    }
    // A `~` is present: the id MUST be the canonical `{team}~{uuid}` form or it is
    // an error — never a silent Local fallback.
    if id.len() < SUFFIX_WITH_SEP_LEN {
        return Err(IdError::Malformed);
    }
    let sep = id.len() - SUFFIX_WITH_SEP_LEN;
    if id.as_bytes()[sep] != b'~' {
        return Err(IdError::Malformed);
    }
    let prefix = &id[..sep];
    let suffix = &id[sep + 1..];
    if !is_valid_team_id(prefix) {
        return Err(IdError::BadTeamPrefix);
    }
    if !is_canonical_v4_uuid(suffix) {
        return Err(IdError::BadUuidSuffix);
    }
    Ok(RouteScope::Team(prefix.to_string()))
}

/// A routable entity id (issue, execution, job, run, turn, event, artifact, …).
/// The write router accepts only this type, so a session id or a provider
/// tool-use id cannot reach the routable parser by accident — the exemptions are
/// TYPED, not conventions.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RoutableId(String);

impl RoutableId {
    /// Wrap a string already known to be a routable id — a value read back from
    /// the database or carried across a trusted boundary. Does not validate;
    /// route resolution happens at [`RoutableId::route_scope`].
    pub fn from_trusted(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    /// Resolve this id's routing scope.
    pub fn route_scope(&self) -> Result<RouteScope, IdError> {
        parse_route_scope(&self.0)
    }
}

impl std::fmt::Display for RoutableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<RoutableId> for String {
    fn from(id: RoutableId) -> String {
        id.0
    }
}

/// A backend conversation/session id. Always a bare v4 UUID and, by type, NEVER
/// routable — `claude --session-id` validates it as a UUID and rejects a
/// `team~uuid` value, and nothing routes a database from a session id alone
/// (sessions route via their owning job/run). The newtype makes that exemption
/// unforgeable: a `BareSessionId` cannot be handed to the routable parser.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BareSessionId(String);

impl BareSessionId {
    pub fn from_trusted(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for BareSessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<BareSessionId> for String {
    fn from(id: BareSessionId) -> String {
        id.0
    }
}

/// Mint a new routable id for the given scope: bare v4 for `Local`,
/// `{team}~{uuid}` for `Team`.
pub fn mint(scope: RouteScope) -> RoutableId {
    let uuid = Uuid::new_v4().to_string();
    match scope {
        RouteScope::Local => RoutableId(uuid),
        RouteScope::Team(team) => RoutableId(format!("{team}~{uuid}")),
    }
}

/// Mint a bare (Local) routable id.
pub fn mint_local() -> RoutableId {
    mint(RouteScope::Local)
}

/// Mint a child id that inherits its parent's scope, so the team prefix
/// propagates down an object graph (job→run→turn→event, artifact, comment) with
/// no route lookup. A parent that fails to parse (a corrupted id — never produced
/// by minting) falls back to Local.
pub fn mint_inheriting(parent: &RoutableId) -> RoutableId {
    mint(parent.route_scope().unwrap_or(RouteScope::Local))
}

/// Mint a bare session id (typed so it can never reach the routable parser).
pub fn mint_session_id() -> BareSessionId {
    BareSessionId(Uuid::new_v4().to_string())
}

/// Drop-in convenience for a child mint site: mint an id that inherits
/// `parent_id`'s scope and return it as a plain `String` for a storage column.
/// Equivalent to `mint_inheriting(&RoutableId::from_trusted(parent_id)).into_string()`.
/// Use this at every ProjectScoped child site so the team prefix propagates down
/// the whole object graph (execution→job→run→turn→event, artifact, comment, …)
/// with no route lookup.
pub fn mint_child(parent_id: &str) -> String {
    mint_inheriting(&RoutableId::from_trusted(parent_id)).into_string()
}

/// Re-key a routable entity id from local/private ownership into `team`.
///
/// This is a pure prefix transform for project moves: bare UUID-bearing ids become
/// `{team}~{uuid}`, ids already carrying the same prefix are returned unchanged
/// so retries are idempotent, and malformed/foreign-prefixed ids fail closed.
/// Bare legacy strings are left untouched because not every ProjectScoped column
/// that participates in routing is uuid-shaped (`projects.id = workspace` in
/// legacy roots, label ids in old data, and URI-visible segments are examples).
pub fn rekey_to_team(id: &str, team: &str) -> Result<String, IdError> {
    match parse_route_scope(id)? {
        RouteScope::Team(existing) => {
            if existing == team {
                Ok(id.to_string())
            } else {
                Err(IdError::BadTeamPrefix)
            }
        }
        RouteScope::Local => {
            if is_canonical_v4_uuid(id) {
                Ok(format!("{team}~{id}"))
            } else {
                Ok(id.to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_uuid() -> String {
        Uuid::new_v4().to_string()
    }

    #[test]
    fn bare_uuid_routes_local() {
        let id = sample_uuid();
        assert_eq!(parse_route_scope(&id), Ok(RouteScope::Local));
    }

    #[test]
    fn legacy_non_uuid_local_ids_route_local() {
        // Project keys and todo positions are bare and not uuids; they must still
        // route Local (lenient bare path), or existing data would orphan.
        assert_eq!(parse_route_scope("CAIRN"), Ok(RouteScope::Local));
        assert_eq!(parse_route_scope("1"), Ok(RouteScope::Local));
        assert_eq!(
            parse_route_scope("some-caller-supplied-key"),
            Ok(RouteScope::Local)
        );
    }

    #[test]
    fn team_prefixed_id_routes_team() {
        let team = "lElEfCpn0UbajBELSHhVEGkO66Hext4o";
        let id = format!("{team}~{}", sample_uuid());
        assert_eq!(
            parse_route_scope(&id),
            Ok(RouteScope::Team(team.to_string()))
        );
    }

    #[test]
    fn uppercase_uuid_suffix_is_rejected() {
        let team = "team1";
        let id = format!("{team}~{}", sample_uuid().to_uppercase());
        assert_eq!(parse_route_scope(&id), Err(IdError::BadUuidSuffix));
    }

    #[test]
    fn bad_charset_prefix_is_rejected() {
        // A tilde at the canonical position but a prefix with a forbidden char.
        let id = format!("team/one~{}", sample_uuid());
        assert_eq!(parse_route_scope(&id), Err(IdError::BadTeamPrefix));
    }

    #[test]
    fn empty_prefix_is_rejected() {
        let id = format!("~{}", sample_uuid());
        assert_eq!(parse_route_scope(&id), Err(IdError::BadTeamPrefix));
    }

    #[test]
    fn tilde_not_at_canonical_position_is_malformed() {
        // A tilde present but the suffix is not 36 chars.
        assert_eq!(parse_route_scope("team~short"), Err(IdError::Malformed));
        // A tilde inside an otherwise-uuid-length tail.
        let id = format!("team~x{}", sample_uuid());
        assert_eq!(parse_route_scope(&id), Err(IdError::Malformed));
    }

    #[test]
    fn non_v4_suffix_is_rejected() {
        // Right length and hyphen layout but version nibble is not 4.
        let id = "team~aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa";
        assert_eq!(parse_route_scope(id), Err(IdError::BadUuidSuffix));
        // Bad variant nibble.
        let id = "team~aaaaaaaa-aaaa-4aaa-caaa-aaaaaaaaaaaa";
        assert_eq!(parse_route_scope(id), Err(IdError::BadUuidSuffix));
    }

    #[test]
    fn team_id_guard() {
        assert!(is_valid_team_id("lElEfCpn0UbajBELSHhVEGkO66Hext4o"));
        assert!(is_valid_team_id("a"));
        assert!(!is_valid_team_id(""));
        assert!(!is_valid_team_id("has space"));
        assert!(!is_valid_team_id("has~tilde"));
        assert!(!is_valid_team_id(&"x".repeat(MAX_TEAM_ID_LEN + 1)));
    }

    #[test]
    fn mint_local_is_bare_and_routes_local() {
        let id = mint_local();
        assert!(!id.as_str().contains('~'));
        assert_eq!(id.route_scope(), Ok(RouteScope::Local));
    }

    #[test]
    fn mint_team_roundtrips() {
        let team = "teamABC123".to_string();
        let id = mint(RouteScope::Team(team.clone()));
        assert!(id.as_str().starts_with(&format!("{team}~")));
        assert_eq!(id.route_scope(), Ok(RouteScope::Team(team)));
    }

    #[test]
    fn mint_inheriting_propagates_team_and_keeps_local_bare() {
        let team = "teamABC123".to_string();
        let parent = mint(RouteScope::Team(team.clone()));
        let child = mint_inheriting(&parent);
        assert_eq!(child.route_scope(), Ok(RouteScope::Team(team)));
        assert_ne!(child.as_str(), parent.as_str());

        let local_parent = mint_local();
        let local_child = mint_inheriting(&local_parent);
        assert_eq!(local_child.route_scope(), Ok(RouteScope::Local));
        assert!(!local_child.as_str().contains('~'));
    }

    #[test]
    fn mint_child_propagates_parent_scope() {
        let team = "teamABC123".to_string();
        let parent = mint(RouteScope::Team(team.clone())).into_string();
        let child = mint_child(&parent);
        assert_eq!(parse_route_scope(&child), Ok(RouteScope::Team(team)));
        let local_child = mint_child(&mint_local().into_string());
        assert!(!local_child.contains('~'));
        assert_eq!(parse_route_scope(&local_child), Ok(RouteScope::Local));
    }

    #[test]
    fn session_id_is_bare_and_parses_local() {
        let sid = mint_session_id();
        assert!(!sid.as_str().contains('~'));
        // A session id, were it ever (wrongly) parsed, routes Local — but the type
        // prevents it reaching the routable parser at all.
        assert_eq!(parse_route_scope(sid.as_str()), Ok(RouteScope::Local));
    }
    #[test]
    fn rekey_to_team_prefixes_bare_uuid_and_preserves_suffix() {
        let team = "Team123";
        let id = sample_uuid();
        let moved = rekey_to_team(&id, team).unwrap();
        assert_eq!(moved, format!("{team}~{id}"));
    }

    #[test]
    fn rekey_to_team_is_idempotent_for_same_team() {
        let team = "Team123";
        let id = format!("{team}~{}", sample_uuid());
        assert_eq!(rekey_to_team(&id, team).unwrap(), id);
    }

    #[test]
    fn rekey_to_team_refuses_foreign_team_prefix() {
        let id = format!("OtherTeam~{}", sample_uuid());
        assert_eq!(rekey_to_team(&id, "Team123"), Err(IdError::BadTeamPrefix));
    }

    #[test]
    fn bare_session_ids_are_protected_by_type_and_manifest_boundaries() {
        let session = mint_session_id();
        assert_eq!(
            rekey_to_team(session.as_str(), "Team123").unwrap(),
            format!("Team123~{}", session.as_str())
        );
        // The transform is intentionally for routable strings. The protection is
        // the type boundary plus the project-move manifest: callers receive
        // `BareSessionId`, and session-id columns are never declared routable.
    }
}
