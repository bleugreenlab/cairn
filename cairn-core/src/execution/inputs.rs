//! What a check's inputs actually ARE.
//!
//! A project check declares its inputs one of two ways. `impact:` names them
//! with globs — right for a lane whose inputs genuinely are a path shape (the
//! migrations lane reads `turso_migrations/**`). `scope:` names a node of the
//! project's own dependency graph — right for a code lane, whose inputs are
//! "this crate and everything it compiles against", a set no hand-maintained
//! glob list stays correct about (CAIRN-3090: `packages/ui` was missing from the
//! typecheck lane's globs, so main went red silently).
//!
//! Both resolve here, to one [`InputSelector`] per check, and that single object
//! answers both questions the checks engine asks about inputs:
//!
//! - **Applicability.** A check applies iff a changed path is one of its inputs.
//! - **Keying.** A check's cached verdict is keyed by the content of exactly its
//!   inputs, so a change outside them reuses the verdict and a change inside
//!   them cannot.
//!
//! Those are the same question asked in opposite directions, so they must not be
//! two mechanisms that can disagree. `selection::plan_one` and
//! `checks::check_command_identity` both consult the selector resolved here.
//!
//! ## The graph comes from the tree, not from a filesystem
//!
//! Planning runs between a sealed commit and the agent's tool result, and agents
//! are virtual: there may be no checkout of the sealed tree anywhere. `cargo
//! metadata` needs a filesystem, and the one filesystem planning *can* reach —
//! the jj store's working copy — sits at whatever revision jj last checked out
//! there, which is not the tree being keyed. Deriving from that would key a
//! verdict by one tree's content under another tree's graph.
//!
//! So the graph is parsed from the sealed tree's own manifest blobs: the
//! workspace `Cargo.toml` member list, each member manifest's dependency tables,
//! the root `package.json` workspaces, each workspace's dependencies, and the
//! `tsconfig.json` path aliases through which TypeScript imports actually cross
//! package boundaries. One `git cat-file --batch` read, no checkout, and a graph
//! that by construction describes the tree it is keying.
//!
//! ## Cost
//!
//! Derivation is memoized on a fingerprint of the manifest blobs alone, so cost
//! scales with the number of distinct manifest STATES — not with history, not
//! with the number of checks, and not with the number of commits. Manifests
//! change rarely; nearly every commit is a memo hit and issues zero subprocesses.
//! That is the CAIRN-3108 planning-latency invariant.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use crate::config::project_settings::{CheckCommand, CheckScopeSelector};
use crate::execution::selection::build_glob_set;

/// How many distinct manifest states to keep derived graphs for. A branch
/// touching manifests plus its base is two; the rest is headroom.
const GRAPH_MEMO_CAPACITY: usize = 8;

// ---------------------------------------------------------------------------
// Tree access
// ---------------------------------------------------------------------------

/// Reads blob CONTENT out of the sealed tree by object id. Abstracted so graph
/// derivation is a pure function of `(entries, blobs)` and unit-testable without
/// a repository.
/// `Send + Sync` because a resolved snapshot is held inside async check
/// planning, and a `&dyn` reference is only `Send` when its referent is `Sync`.
pub(crate) trait BlobReader: Send + Sync {
    fn read(&self, ids: &[&str]) -> Result<HashMap<String, Vec<u8>>, String>;
}

/// A reader that has nothing to read. Every derivation degrades to the
/// conservative whole-tree selector against it, which is the same fallback an
/// unreadable tree takes. Only the treeless test path needs it: every
/// production caller has a real store behind it.
#[cfg(test)]
pub(crate) struct NoBlobs;

#[cfg(test)]
impl BlobReader for NoBlobs {
    fn read(&self, _ids: &[&str]) -> Result<HashMap<String, Vec<u8>>, String> {
        Ok(HashMap::new())
    }
}

#[cfg(test)]
static NO_BLOBS: NoBlobs = NoBlobs;

/// Reads blobs out of the jj store's git object database. This is the real
/// reader: it needs no checkout, so derivation works for a virtual agent whose
/// sealed tree is materialized nowhere.
pub(crate) struct TreeBlobs<'a> {
    pub(crate) jj: &'a crate::jj::JjEnv,
    pub(crate) repository: &'a std::path::Path,
}

impl BlobReader for TreeBlobs<'_> {
    fn read(&self, ids: &[&str]) -> Result<HashMap<String, Vec<u8>>, String> {
        crate::jj::read_blobs(self.jj, self.repository, ids)
    }
}

/// The sealed tree as graph derivation needs it: the flat `(path, blob_id)`
/// listing (already fetched by the check runners for cache keying) plus a way to
/// read the manifest blobs those entries point at.
pub(crate) struct TreeSnapshot<'a> {
    entries: Option<&'a [(String, String)]>,
    blobs: &'a dyn BlobReader,
}

impl<'a> TreeSnapshot<'a> {
    pub(crate) fn new(entries: Option<&'a [(String, String)]>, blobs: &'a dyn BlobReader) -> Self {
        Self { entries, blobs }
    }

    /// A snapshot of nothing — no entries, no blobs. Every declared selector
    /// degrades to whole-tree keying against it.
    #[cfg(test)]
    pub(crate) fn empty() -> TreeSnapshot<'static> {
        TreeSnapshot {
            entries: None,
            blobs: &NO_BLOBS,
        }
    }
}

// ---------------------------------------------------------------------------
// The selector
// ---------------------------------------------------------------------------

enum SelectorKind {
    /// No selector declared: every path is an input and the key is the whole
    /// sealed tree hash.
    Everything,
    /// A selector WAS declared but could not be resolved — an uncompilable glob,
    /// an unreadable tree, an unknown scope token. Matches every path and keys on
    /// the whole tree: over-invalidate, never falsely reuse.
    Unresolved,
    Globs(globset::GlobSet),
    Closure(ClosureMatcher),
}

/// One check's resolved input set: a path predicate plus the definition text
/// that enters its cache key.
pub(crate) struct InputSelector {
    kind: SelectorKind,
    definition: Vec<String>,
    config_error: Option<String>,
}

impl InputSelector {
    /// The selector for a check that declares no inputs.
    pub(crate) fn everything() -> Self {
        Self {
            kind: SelectorKind::Everything,
            definition: Vec::new(),
            config_error: None,
        }
    }

    /// A glob selector, for tests and for callers holding raw globs.
    pub(crate) fn from_globs(globs: &[String]) -> Self {
        let mut definition = globs.to_vec();
        definition.sort();
        match build_glob_set(globs) {
            Ok(set) => Self {
                kind: SelectorKind::Globs(set),
                definition,
                config_error: None,
            },
            Err(_) => Self {
                kind: SelectorKind::Unresolved,
                definition,
                config_error: None,
            },
        }
    }

    fn unresolved(definition: Vec<String>) -> Self {
        Self {
            kind: SelectorKind::Unresolved,
            definition,
            config_error: None,
        }
    }

    /// Whether `path` is one of this check's inputs.
    pub(crate) fn matches(&self, path: &str) -> bool {
        match &self.kind {
            SelectorKind::Everything | SelectorKind::Unresolved => true,
            SelectorKind::Globs(set) => set.is_match(path),
            SelectorKind::Closure(matcher) => matcher.matches(path),
        }
    }

    /// The sorted strings hashed into the result key alongside the filtered tree
    /// content. For a glob selector these are the globs; for a closure selector
    /// they are the scope tokens PLUS the resolved node names PLUS the applicable
    /// extra-input globs, so a closure that widens (a graph-parser fix, a new
    /// dependency edge) cannot reuse a verdict computed under the narrower one.
    pub(crate) fn definition(&self) -> &[String] {
        &self.definition
    }

    /// Whether this check's key is the whole sealed tree hash. True only when
    /// nothing was declared; a declared-but-unresolved selector hashes the whole
    /// entry list instead, which is the same conservative answer by a route that
    /// keeps the selector's definition in the key.
    pub(crate) fn keys_on_whole_tree(&self) -> bool {
        matches!(self.kind, SelectorKind::Everything)
    }

    /// Whether the check declared inputs at all — true even when the declaration
    /// would not resolve, which is what makes an unresolvable selector apply
    /// conservatively instead of reading as "no inputs declared".
    pub(crate) fn is_declared(&self) -> bool {
        !matches!(self.kind, SelectorKind::Everything)
    }

    /// Whether the selector genuinely narrows, so the planner may hand a
    /// `{changedFiles}`/`{targets}` placeholder the matched subset. An
    /// unresolved selector does not: it degrades to the whole-suite command.
    pub(crate) fn narrows(&self) -> bool {
        matches!(self.kind, SelectorKind::Globs(_) | SelectorKind::Closure(_))
    }

    /// Set when the DECLARATION itself is invalid, which makes the check
    /// unrunnable rather than merely un-narrowed.
    pub(crate) fn config_error(&self) -> Option<&str> {
        self.config_error.as_deref()
    }
}

impl std::fmt::Debug for InputSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match &self.kind {
            SelectorKind::Everything => "everything",
            SelectorKind::Unresolved => "unresolved",
            SelectorKind::Globs(_) => "globs",
            SelectorKind::Closure(_) => "closure",
        };
        f.debug_struct("InputSelector")
            .field("kind", &kind)
            .field("definition", &self.definition)
            .field("config_error", &self.config_error)
            .finish()
    }
}

/// Every check's selector, resolved once per planning pass against one tree.
pub(crate) struct ResolvedInputs {
    selectors: HashMap<String, InputSelector>,
    extra_inputs: HashMap<String, Vec<String>>,
}

static EVERYTHING: OnceLock<InputSelector> = OnceLock::new();

impl ResolvedInputs {
    /// Resolve every check's selector against `tree`. The project graph is
    /// derived at most once for the whole pass, and only when some check needs it.
    pub(crate) fn resolve(
        checks: &HashMap<String, CheckCommand>,
        extra_inputs: &HashMap<String, Vec<String>>,
        tree: &TreeSnapshot<'_>,
    ) -> Self {
        let graph = if checks.values().any(|check| check.scope.is_some()) {
            project_graph(tree)
        } else {
            None
        };
        let selectors = checks
            .iter()
            .map(|(name, check)| {
                (
                    name.clone(),
                    resolve_selector(check, extra_inputs, graph.as_ref()),
                )
            })
            .collect();
        Self {
            selectors,
            extra_inputs: extra_inputs.clone(),
        }
    }

    pub(crate) fn for_check(&self, name: &str) -> &InputSelector {
        self.selectors
            .get(name)
            .unwrap_or_else(|| EVERYTHING.get_or_init(InputSelector::everything))
    }

    /// Re-resolve ONE check's selector against a different tree.
    ///
    /// The narrowing baseline compares a cached green tree with the current one,
    /// and the honest comparison uses each tree's OWN graph — the closure is a
    /// pure function of the tree, so a baseline whose manifests differ has a
    /// different closure and must be judged under it.
    pub(crate) fn resolve_for_tree(
        &self,
        check: &CheckCommand,
        tree: &TreeSnapshot<'_>,
    ) -> InputSelector {
        resolve_one(check, &self.extra_inputs, tree)
    }
}

/// Resolve ONE check's selector against a tree, with no pass-wide map. The
/// graph is derived only when the check actually names a node in it.
pub(crate) fn resolve_one(
    check: &CheckCommand,
    extra_inputs: &HashMap<String, Vec<String>>,
    tree: &TreeSnapshot<'_>,
) -> InputSelector {
    let graph = if check.scope.is_some() {
        project_graph(tree)
    } else {
        None
    };
    resolve_selector(check, extra_inputs, graph.as_ref())
}

/// Whether any configured check declares inputs at all. The check runners fetch
/// the sealed tree's entry listing only when this holds — with no declared
/// selector anywhere, every key is the whole-tree hash and the listing is dead
/// weight.
pub(crate) fn any_check_declares_inputs<'a>(
    checks: impl IntoIterator<Item = &'a CheckCommand>,
) -> bool {
    checks
        .into_iter()
        .any(|check| check.impact.is_some() || check.scope.is_some())
}

fn resolve_selector(
    check: &CheckCommand,
    extra_inputs: &HashMap<String, Vec<String>>,
    graph: Option<&Arc<ProjectGraph>>,
) -> InputSelector {
    match (check.impact.as_ref(), check.scope.as_ref()) {
        (Some(globs), Some(scope)) => {
            // One mechanism per lane. Both declared means the project asked two
            // different questions about the same check's inputs and there is no
            // honest answer to run under, so the check reports a config error and
            // caches nothing — the same treatment an unexpandable placeholder gets.
            let mut definition = globs.clone();
            definition.extend(scope.tokens().iter().map(|token| format!("scope:{token}")));
            definition.sort();
            InputSelector {
                kind: SelectorKind::Unresolved,
                definition,
                config_error: Some(format!(
                    "check declares both `impact` ({}) and `scope` ({}). They are two \
                     different definitions of the same check's inputs and cannot both \
                     hold. Declare `scope` for a code lane whose inputs are a dependency \
                     closure, or `impact` for a lane whose inputs genuinely are a path \
                     shape. Nothing was run.",
                    globs.join(", "),
                    scope.tokens().join(", ")
                )),
            }
        }
        (Some(globs), None) => InputSelector::from_globs(globs),
        (None, Some(scope)) => resolve_scope(scope, extra_inputs, graph),
        (None, None) => InputSelector::everything(),
    }
}

fn resolve_scope(
    scope: &CheckScopeSelector,
    extra_inputs: &HashMap<String, Vec<String>>,
    graph: Option<&Arc<ProjectGraph>>,
) -> InputSelector {
    let tokens = scope.tokens();
    let mut definition: Vec<String> = tokens
        .iter()
        .map(|token| format!("scope:{token}"))
        .collect();
    definition.sort();

    let Some(graph) = graph else {
        return InputSelector::unresolved(definition);
    };

    // Every token must name a real node in one domain. A scope list spanning two
    // domains has no single ownership rule, and an unknown node name means the
    // graph and the config disagree — both resolve conservatively rather than
    // silently selecting a smaller input set than declared.
    let mut resolved: Option<(Domain, Arc<DomainGraph>)> = None;
    let mut seeds: BTreeSet<String> = BTreeSet::new();
    for token in &tokens {
        let Some((token_domain, node)) = parse_scope_token(token) else {
            log::warn!("check scope token {token:?} is not `rust:<crate>` or `ts:<package>`");
            return InputSelector::unresolved(definition);
        };
        let nodes = match &resolved {
            Some((domain, nodes)) if *domain == token_domain => Arc::clone(nodes),
            Some(_) => {
                log::warn!("check scope {tokens:?} spans more than one domain");
                return InputSelector::unresolved(definition);
            }
            None => {
                // A domain whose derivation hit anything it could not read is
                // absent entirely rather than partial, so its scopes land here.
                let Some(nodes) = graph.domain(token_domain) else {
                    log::warn!(
                        "check scope token {token:?} names a domain whose graph could not be derived"
                    );
                    return InputSelector::unresolved(definition);
                };
                resolved = Some((token_domain, Arc::clone(nodes)));
                Arc::clone(nodes)
            }
        };
        if !nodes.nodes.contains_key(node) {
            log::warn!("check scope token {token:?} names no node in the derived project graph");
            return InputSelector::unresolved(definition);
        }
        seeds.insert(node.to_string());
    }
    let Some((domain, nodes)) = resolved else {
        return InputSelector::unresolved(definition);
    };

    let closure = nodes.forward_closure(&seeds);
    // A node whose file roots could not be derived makes ownership — and so the
    // whole closure's input set — unknowable.
    if closure.iter().any(|name| nodes.nodes[name].roots_unknown) {
        log::warn!("check scope {tokens:?} reaches a node with underivable file roots");
        return InputSelector::unresolved(definition);
    }

    // Extra inputs attach to a NODE, not to a check, so they compose transitively
    // to every check whose closure reaches that node: declaring the migration SQL
    // as an input of `rust:cairn-db` puts it in `rust:cairn-core`'s closure too,
    // because cairn-core's tests compile those migrations in.
    let mut extra: Vec<String> = Vec::new();
    for name in &closure {
        if let Some(globs) = extra_inputs.get(&format!("{}:{name}", domain.prefix())) {
            extra.extend(globs.iter().cloned());
        }
    }
    extra.sort();
    extra.dedup();

    definition.extend(closure.iter().map(|name| format!("closure:{name}")));
    definition.extend(extra.iter().map(|glob| format!("extra:{glob}")));
    definition.sort();

    let extra_set = if extra.is_empty() {
        None
    } else {
        match build_glob_set(&extra) {
            Ok(set) => Some(set),
            Err(error) => {
                log::warn!("check scope extra inputs {extra:?} would not compile: {error}");
                return InputSelector::unresolved(definition);
            }
        }
    };

    InputSelector {
        kind: SelectorKind::Closure(ClosureMatcher {
            nodes,
            closure,
            extra: extra_set,
        }),
        definition,
        config_error: None,
    }
}

fn parse_scope_token(token: &str) -> Option<(Domain, &str)> {
    let (prefix, node) = token.split_once(':')?;
    let domain = match prefix.trim() {
        "rust" => Domain::Rust,
        "ts" => Domain::Ts,
        _ => return None,
    };
    let node = node.trim();
    (!node.is_empty()).then_some((domain, node))
}

// ---------------------------------------------------------------------------
// Closure matching
// ---------------------------------------------------------------------------

/// The path predicate of a resolved `scope`. It holds the domain graph directly:
/// a matcher exists only for a domain that derived completely, so there is no
/// partial-graph state for matching to reach.
struct ClosureMatcher {
    nodes: Arc<DomainGraph>,
    closure: BTreeSet<String>,
    extra: Option<globset::GlobSet>,
}

impl ClosureMatcher {
    fn matches(&self, path: &str) -> bool {
        if self.nodes.globals.contains(path) {
            return true;
        }
        if self.extra.as_ref().is_some_and(|set| set.is_match(path)) {
            return true;
        }
        // Ownership is longest-prefix-wins across EVERY node in the domain, not
        // just the closure's. The Rust workspace root member's directory
        // (`src-tauri/`) contains every other member, so a shorter-prefix match
        // would attribute all of them to the app crate.
        self.nodes
            .owner(path)
            .is_some_and(|owner| self.closure.contains(owner))
    }
}

// ---------------------------------------------------------------------------
// The graph
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Domain {
    Rust,
    Ts,
}

impl Domain {
    fn prefix(self) -> &'static str {
        match self {
            Domain::Rust => "rust",
            Domain::Ts => "ts",
        }
    }
}

#[derive(Debug, Default)]
struct DomainNode {
    /// Path prefixes this node owns, each ending in `/`.
    roots: Vec<String>,
    /// True when the node exists but its file roots could not be derived.
    roots_unknown: bool,
    /// Names of nodes in the same domain this one depends on, directly.
    deps: BTreeSet<String>,
}

#[derive(Debug, Default)]
struct DomainGraph {
    nodes: BTreeMap<String, DomainNode>,
    /// Exact paths whose change invalidates every node in the domain.
    globals: BTreeSet<String>,
}

impl DomainGraph {
    fn owner(&self, path: &str) -> Option<&str> {
        let mut best: Option<(&str, usize)> = None;
        for (name, node) in &self.nodes {
            for root in &node.roots {
                if path.starts_with(root.as_str())
                    && best.is_none_or(|(_, length)| root.len() > length)
                {
                    best = Some((name.as_str(), root.len()));
                }
            }
        }
        best.map(|(name, _)| name)
    }

    fn forward_closure(&self, seeds: &BTreeSet<String>) -> BTreeSet<String> {
        let mut out: BTreeSet<String> = BTreeSet::new();
        let mut stack: Vec<String> = seeds.iter().cloned().collect();
        while let Some(name) = stack.pop() {
            if !out.insert(name.clone()) {
                continue;
            }
            if let Some(node) = self.nodes.get(&name) {
                for dep in &node.deps {
                    if !out.contains(dep) {
                        stack.push(dep.clone());
                    }
                }
            }
        }
        out
    }
}

/// The project's dependency graph as derived from one tree's manifests.
///
/// A domain is `None` when anything its derivation treats as authoritative could
/// not be read or parsed. That distinction is the whole point: a domain graph
/// built from the manifests that happened to parse is not a smaller description
/// of the project, it is a WRONG one. A member whose manifest is unreadable
/// vanishes from the member set, so every edge into it is filtered away as
/// external and its directory falls to whichever member's prefix is next
/// longest — and a check scoped to one of its dependents then reuses a verdict
/// that never examined those files. Absent means every scope in the domain keys
/// on the whole tree; partial would mean silently under-invalidating. Domains
/// fail independently, so a malformed `Cargo.toml` costs `ts:` checks nothing.
#[derive(Debug, Default)]
pub(crate) struct ProjectGraph {
    rust: Option<Arc<DomainGraph>>,
    ts: Option<Arc<DomainGraph>>,
}

impl ProjectGraph {
    fn domain(&self, domain: Domain) -> Option<&Arc<DomainGraph>> {
        match domain {
            Domain::Rust => self.rust.as_ref(),
            Domain::Ts => self.ts.as_ref(),
        }
    }
}

// ---------------------------------------------------------------------------
// Derivation
// ---------------------------------------------------------------------------

/// Paths whose content defines the graph. Everything else in the tree can change
/// without moving a single edge, which is exactly what makes the memo below
/// scale with manifest states rather than with commits.
fn is_manifest(path: &str) -> bool {
    if path.starts_with("node_modules/") || path.contains("/node_modules/") {
        return false;
    }
    let base = path.rsplit('/').next().unwrap_or(path);
    base == "Cargo.toml"
        || base == "package.json"
        || (base.starts_with("tsconfig") && base.ends_with(".json"))
}

fn manifest_fingerprint(manifests: &[(&str, &str)]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for (path, blob) in manifests {
        hasher.update(path.as_bytes());
        hasher.update([0u8]);
        hasher.update(blob.as_bytes());
        hasher.update([0u8]);
    }
    format!("{:x}", hasher.finalize())
}

type GraphMemo = Mutex<Vec<(String, Arc<ProjectGraph>)>>;
static GRAPH_MEMO: OnceLock<GraphMemo> = OnceLock::new();

fn memo() -> &'static GraphMemo {
    GRAPH_MEMO.get_or_init(|| Mutex::new(Vec::new()))
}

/// The dependency graph of the tree in `snapshot`, derived from its manifest
/// blobs and memoized on their fingerprint. `None` when the tree cannot be read
/// at all, which degrades every scope selector to whole-tree keying.
fn project_graph(snapshot: &TreeSnapshot<'_>) -> Option<Arc<ProjectGraph>> {
    project_graph_memoized(snapshot, memo())
}

/// The memo is a parameter so a test can supply its own. The shared one is
/// process-global and bounded, so a test asserting reuse through it would be
/// measuring what the rest of the suite derived in parallel rather than what
/// this function does.
fn project_graph_memoized(
    snapshot: &TreeSnapshot<'_>,
    memo: &GraphMemo,
) -> Option<Arc<ProjectGraph>> {
    let entries = snapshot.entries?;
    let manifests: Vec<(&str, &str)> = entries
        .iter()
        .filter(|(path, _)| is_manifest(path))
        .map(|(path, blob)| (path.as_str(), blob.as_str()))
        .collect();
    if manifests.is_empty() {
        return None;
    }
    let fingerprint = manifest_fingerprint(&manifests);

    if let Ok(cache) = memo.lock() {
        if let Some((_, graph)) = cache.iter().find(|(key, _)| key == &fingerprint) {
            return Some(Arc::clone(graph));
        }
    }

    let ids: Vec<&str> = {
        let mut ids: Vec<&str> = manifests.iter().map(|(_, blob)| *blob).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    };
    let blobs = match snapshot.blobs.read(&ids) {
        Ok(blobs) => blobs,
        Err(error) => {
            log::warn!("check input derivation: could not read manifest blobs: {error}");
            return None;
        }
    };
    // Every manifest must materialize. Dropping the ones that fail would hand
    // derivation a tree that is missing exactly the files that define the graph.
    let mut sources: BTreeMap<&str, &str> = BTreeMap::new();
    for (path, blob) in &manifests {
        let Some(bytes) = blobs.get(*blob) else {
            log::warn!(
                "check input derivation: manifest {path} is absent from the object database"
            );
            return None;
        };
        let Ok(text) = std::str::from_utf8(bytes) else {
            log::warn!("check input derivation: manifest {path} is not valid UTF-8");
            return None;
        };
        sources.insert(*path, text);
    }

    let graph = Arc::new(ProjectGraph {
        rust: derive_rust_graph(&sources).map(Arc::new),
        ts: derive_ts_graph(&sources).map(Arc::new),
    });

    if let Ok(mut cache) = memo.lock() {
        cache.retain(|(key, _)| key != &fingerprint);
        cache.push((fingerprint, Arc::clone(&graph)));
        let overflow = cache.len().saturating_sub(GRAPH_MEMO_CAPACITY);
        cache.drain(..overflow);
    }
    Some(graph)
}

/// Global invalidators of the Rust domain: the workspace manifest (which defines
/// the member list and shared profiles), the lockfile, the cargo config, and a
/// pinned toolchain at either the repo root or the workspace root.
const RUST_GLOBALS: &[&str] = &[
    "src-tauri/Cargo.toml",
    "src-tauri/Cargo.lock",
    "src-tauri/.cargo/config.toml",
    "src-tauri/rust-toolchain",
    "src-tauri/rust-toolchain.toml",
    "rust-toolchain",
    "rust-toolchain.toml",
];

const RUST_WORKSPACE_MANIFEST: &str = "src-tauri/Cargo.toml";
const RUST_WORKSPACE_ROOT: &str = "src-tauri";

/// The Rust domain, or `None` if any part of the member list could not be read.
///
/// Every declared member is authoritative: the member set decides which
/// dependency names count as internal edges and which directories are owned, so
/// one member that does not resolve corrupts the graph for members that did.
/// A member path containing a glob (which Cargo permits) also lands here, since
/// this resolves member paths literally.
fn derive_rust_graph(sources: &BTreeMap<&str, &str>) -> Option<DomainGraph> {
    let mut graph = DomainGraph {
        globals: RUST_GLOBALS.iter().map(|path| path.to_string()).collect(),
        ..Default::default()
    };
    let workspace_text = sources.get(RUST_WORKSPACE_MANIFEST)?;
    let Ok(workspace) = workspace_text.parse::<toml::Value>() else {
        log::warn!("check input derivation: {RUST_WORKSPACE_MANIFEST} would not parse");
        return None;
    };
    // The explicit member list is authoritative. Globbing for `Cargo.toml` under
    // the workspace root would pick up `src-tauri/os/Cargo.toml`, which exists
    // and is NOT a member.
    let members = workspace
        .get("workspace")
        .and_then(|ws| ws.get("members"))
        .and_then(|members| members.as_array())?;

    // Pass one: every member's name and manifest directory.
    let mut parsed: Vec<(String, String, toml::Value)> = Vec::new();
    for member in members {
        let Some(member) = member.as_str() else {
            log::warn!("check input derivation: a workspace member entry is not a string");
            return None;
        };
        let dir = if member == "." {
            RUST_WORKSPACE_ROOT.to_string()
        } else {
            format!("{RUST_WORKSPACE_ROOT}/{}", member.trim_end_matches('/'))
        };
        let manifest_path = format!("{dir}/Cargo.toml");
        let Some(text) = sources.get(manifest_path.as_str()) else {
            log::warn!(
                "check input derivation: workspace member manifest {manifest_path} \
                 is absent from the tree"
            );
            return None;
        };
        let Ok(doc) = text.parse::<toml::Value>() else {
            log::warn!("check input derivation: {manifest_path} would not parse");
            return None;
        };
        let Some(name) = doc
            .get("package")
            .and_then(|package| package.get("name"))
            .and_then(|name| name.as_str())
        else {
            log::warn!("check input derivation: {manifest_path} declares no package name");
            return None;
        };
        parsed.push((name.to_string(), dir, doc));
    }

    let member_names: BTreeSet<&str> = parsed.iter().map(|(name, _, _)| name.as_str()).collect();

    // Pass two: internal edges, now that every member name is known.
    for (name, dir, doc) in &parsed {
        let mut declared = BTreeSet::new();
        collect_cargo_deps(doc, &mut declared);
        let deps = declared
            .into_iter()
            .filter(|dep| dep != name && member_names.contains(dep.as_str()))
            .collect();
        graph.nodes.insert(
            name.clone(),
            DomainNode {
                roots: vec![format!("{dir}/")],
                roots_unknown: false,
                deps,
            },
        );
    }
    Some(graph)
}

/// Every workspace-internal edge in a Cargo manifest is a name in a dependency
/// table, whichever table it sits in: a `[dev-dependencies]` edge is real (the
/// crate's own tests compile against it), a `[build-dependencies]` edge is real
/// (its build script does), and a `[target.'cfg(unix)'.dependencies]` edge is
/// real on the platforms the check runs on.
fn collect_cargo_deps(doc: &toml::Value, out: &mut BTreeSet<String>) {
    const TABLES: &[&str] = &["dependencies", "dev-dependencies", "build-dependencies"];
    for table in TABLES {
        if let Some(table) = doc.get(table).and_then(|value| value.as_table()) {
            push_cargo_deps(table, out);
        }
    }
    if let Some(targets) = doc.get("target").and_then(|value| value.as_table()) {
        for cfg in targets.values() {
            for table in TABLES {
                if let Some(table) = cfg.get(table).and_then(|value| value.as_table()) {
                    push_cargo_deps(table, out);
                }
            }
        }
    }
}

fn push_cargo_deps(table: &toml::Table, out: &mut BTreeSet<String>) {
    for (key, value) in table {
        // `renamed = { package = "real-name", ... }` depends on `real-name`.
        let name = value
            .as_table()
            .and_then(|detail| detail.get("package"))
            .and_then(|package| package.as_str())
            .unwrap_or(key.as_str());
        out.insert(name.to_string());
    }
}

/// Global invalidators of the TypeScript domain. The root `package.json` defines
/// the workspace set, the lockfile pins every resolved version, and the bundler
/// and test-runner configs govern every workspace's compilation. Root-level
/// `tsconfig*.json` files are added from the tree in [`derive_ts_graph`].
const TS_GLOBALS: &[&str] = &[
    "package.json",
    "bun.lock",
    "bun.lockb",
    "vite.config.ts",
    "vitest.config.ts",
];

const TS_ROOT_MANIFEST: &str = "package.json";
const TS_ROOT_TSCONFIG: &str = "tsconfig.json";

/// The TypeScript domain, or `None` if a manifest or tsconfig that defines it
/// could not be read. Same rule as the Rust side: a workspace we cannot name or
/// an alias we cannot see would narrow a closure without saying so.
fn derive_ts_graph(sources: &BTreeMap<&str, &str>) -> Option<DomainGraph> {
    let mut graph = DomainGraph {
        globals: TS_GLOBALS.iter().map(|path| path.to_string()).collect(),
        ..Default::default()
    };
    // Every root-level tsconfig governs the root compilation, whatever it is named.
    for path in sources.keys() {
        if !path.contains('/') && path.starts_with("tsconfig") && path.ends_with(".json") {
            graph.globals.insert((*path).to_string());
        }
    }

    // Parse every tsconfig up front, because their `paths` aliases are edges: a
    // TypeScript import crosses a package boundary through an alias whether or
    // not a dependency is declared. One that will not parse means edges we
    // cannot see, not edges that are absent.
    let mut tsconfigs: BTreeMap<&str, serde_json::Value> = BTreeMap::new();
    for (path, text) in sources {
        let base = path.rsplit('/').next().unwrap_or(path);
        if !(base.starts_with("tsconfig") && base.ends_with(".json")) {
            continue;
        }
        let Ok(doc) = json5::from_str::<serde_json::Value>(text) else {
            log::warn!("check input derivation: {path} would not parse");
            return None;
        };
        tsconfigs.insert(*path, doc);
    }

    let root_text = sources.get(TS_ROOT_MANIFEST)?;
    let Ok(root) = json5::from_str::<serde_json::Value>(root_text) else {
        log::warn!("check input derivation: {TS_ROOT_MANIFEST} would not parse");
        return None;
    };
    let Some(root_name) = root.get("name").and_then(|name| name.as_str()) else {
        log::warn!("check input derivation: {TS_ROOT_MANIFEST} declares no package name");
        return None;
    };

    let patterns = workspace_patterns(&root)?;
    let workspace_matcher = build_workspace_matcher(&patterns)?;

    // (package name, package directory, parsed manifest). The root package's
    // directory is the repo root, so it is kept out of the prefix rule below and
    // gets its roots from the root tsconfig's `include` instead.
    let mut packages: Vec<(String, String, serde_json::Value)> = Vec::new();
    for (path, text) in sources {
        let Some(dir) = path.strip_suffix("/package.json") else {
            continue;
        };
        if !workspace_matcher.is_match(dir) {
            continue;
        }
        // A matched workspace owns a directory whether or not we can read it, so
        // one we cannot name is a directory whose files would fall to another
        // node by prefix and be keyed under the wrong closure.
        let Ok(doc) = json5::from_str::<serde_json::Value>(text) else {
            log::warn!("check input derivation: {path} would not parse");
            return None;
        };
        let Some(name) = doc.get("name").and_then(|name| name.as_str()) else {
            log::warn!("check input derivation: workspace manifest {path} declares no name");
            return None;
        };
        packages.push((name.to_string(), dir.to_string(), doc));
    }

    let (root_roots, root_roots_unknown) = match tsconfigs.get(TS_ROOT_TSCONFIG) {
        Some(doc) => match tsconfig_include_roots(doc) {
            Some(roots) if !roots.is_empty() => (roots, false),
            _ => (Vec::new(), true),
        },
        None => (Vec::new(), true),
    };
    graph.nodes.insert(
        root_name.to_string(),
        DomainNode {
            roots: root_roots,
            roots_unknown: root_roots_unknown,
            deps: BTreeSet::new(),
        },
    );
    for (name, dir, _) in &packages {
        graph.nodes.insert(
            name.clone(),
            DomainNode {
                roots: vec![format!("{dir}/")],
                roots_unknown: false,
                deps: BTreeSet::new(),
            },
        );
    }

    let package_names: BTreeSet<String> = graph.nodes.keys().cloned().collect();
    // Package directories, longest first, for resolving a tsconfig alias target
    // and a tsconfig's own owner onto a workspace.
    let mut dirs: Vec<(String, String)> = packages
        .iter()
        .map(|(name, dir, _)| (format!("{dir}/"), name.clone()))
        .collect();
    dirs.sort_by_key(|(dir, _)| std::cmp::Reverse(dir.len()));

    // Declared dependency edges.
    for (name, doc) in std::iter::once((root_name.to_string(), &root))
        .chain(packages.iter().map(|(name, _, doc)| (name.clone(), doc)))
    {
        let mut deps = BTreeSet::new();
        for table in ["dependencies", "devDependencies", "peerDependencies"] {
            let Some(table) = doc.get(table).and_then(|value| value.as_object()) else {
                continue;
            };
            for key in table.keys() {
                if key != &name && package_names.contains(key.as_str()) {
                    deps.insert(key.clone());
                }
            }
        }
        if let Some(node) = graph.nodes.get_mut(&name) {
            node.deps.extend(deps);
        }
    }

    // Alias edges. TypeScript imports cross package boundaries through tsconfig
    // `paths`, not only through package.json dependencies, so an alias target
    // landing inside another workspace IS an edge — this is the mechanism a
    // hand-written glob list has no way to see.
    for (path, doc) in &tsconfigs {
        let config_dir = path.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("");
        let owner = dirs
            .iter()
            .find(|(dir, _)| {
                config_dir == dir.trim_end_matches('/') || config_dir.starts_with(dir.as_str())
            })
            .map(|(_, name)| name.clone())
            .unwrap_or_else(|| root_name.to_string());
        for target in tsconfig_alias_targets(doc) {
            let resolved = join_relative(config_dir, &target);
            let Some((_, aliased)) = dirs
                .iter()
                .find(|(dir, _)| resolved.starts_with(dir.as_str()))
            else {
                continue;
            };
            if aliased == &owner {
                continue;
            }
            let aliased = aliased.clone();
            if let Some(node) = graph.nodes.get_mut(&owner) {
                node.deps.insert(aliased);
            }
        }
    }

    Some(graph)
}

/// The declared workspace patterns, or `None` if the declaration is not one this
/// understands in full. `workspaces` decides which package manifests become
/// nodes at all, so a pattern dropped here removes a real workspace from the
/// graph and every edge into it — the same silent narrowing a missing Cargo
/// member causes. A project declaring no workspaces is not that: it is an
/// ordinary root-only project, and yields an empty pattern list.
fn workspace_patterns(root: &serde_json::Value) -> Option<Vec<String>> {
    let array = match root.get("workspaces") {
        None | Some(serde_json::Value::Null) => return Some(Vec::new()),
        Some(serde_json::Value::Array(array)) => array,
        // The object form is npm's, where the patterns live under `packages`.
        Some(serde_json::Value::Object(object)) => match object.get("packages") {
            None => return Some(Vec::new()),
            Some(serde_json::Value::Array(array)) => array,
            Some(_) => {
                log::warn!("check input derivation: `workspaces.packages` is not an array");
                return None;
            }
        },
        Some(_) => {
            log::warn!("check input derivation: `workspaces` is neither an array nor an object");
            return None;
        }
    };
    array
        .iter()
        .map(|value| {
            let Some(pattern) = value.as_str() else {
                log::warn!("check input derivation: a `workspaces` entry is not a string");
                return None;
            };
            Some(pattern.trim_end_matches('/').to_string())
        })
        .collect()
}

/// Workspace patterns are matched with npm/bun semantics, where `packages/*` is
/// one directory level — unlike the `impact` glob matcher, which deliberately
/// lets `*` cross separators.
///
/// `None` when any pattern will not compile, which fails the domain. Compiling
/// the subset that happened to be valid would silently drop the workspaces the
/// rest select. An empty list compiles to a matcher that matches nothing, which
/// is the honest answer for a project with no workspaces.
fn build_workspace_matcher(patterns: &[String]) -> Option<globset::GlobSet> {
    let mut builder = globset::GlobSetBuilder::new();
    for pattern in patterns {
        let glob = globset::GlobBuilder::new(pattern)
            .literal_separator(true)
            .build()
            .map_err(|error| {
                log::warn!("check input derivation: workspace pattern {pattern:?}: {error}")
            })
            .ok()?;
        builder.add(glob);
    }
    builder.build().ok()
}

/// Directory roots a tsconfig's `include` selects, normalized to `dir/` prefixes.
/// `None` when it declares no `include`, which makes the owning node's roots
/// unknown. A file that will not parse never reaches here — it fails the domain.
fn tsconfig_include_roots(doc: &serde_json::Value) -> Option<Vec<String>> {
    let include = doc.get("include")?.as_array()?;
    let mut roots: Vec<String> = include
        .iter()
        .filter_map(|value| value.as_str())
        .map(literal_prefix)
        .filter(|root| !root.is_empty())
        .collect();
    roots.sort();
    roots.dedup();
    Some(roots)
}

/// The literal directory prefix of an include/alias pattern: everything up to
/// the first wildcard, trimmed back to a directory boundary and terminated with
/// `/`. `src` → `src/`; `src/**/*.ts` → `src/`.
fn literal_prefix(pattern: &str) -> String {
    let pattern = pattern.trim_start_matches("./");
    let literal = match pattern.find(['*', '?']) {
        Some(index) => match pattern[..index].rfind('/') {
            Some(slash) => &pattern[..slash],
            None => "",
        },
        None => pattern,
    };
    let literal = literal.trim_end_matches('/');
    if literal.is_empty() {
        String::new()
    } else {
        format!("{literal}/")
    }
}

fn tsconfig_alias_targets(doc: &serde_json::Value) -> Vec<String> {
    let Some(paths) = doc
        .get("compilerOptions")
        .and_then(|options| options.get("paths"))
        .and_then(|paths| paths.as_object())
    else {
        return Vec::new();
    };
    paths
        .values()
        .filter_map(|value| value.as_array())
        .flatten()
        .filter_map(|value| value.as_str())
        .map(str::to_string)
        .collect()
}

/// Resolve a tsconfig-relative target against the config's own directory.
fn join_relative(dir: &str, target: &str) -> String {
    let target = target.trim_start_matches("./");
    if dir.is_empty() {
        target.to_string()
    } else {
        format!("{dir}/{target}")
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// An in-memory sealed tree: `(path, blob_id)` entries plus the bytes those
    /// ids point at, with a read counter so the derivation memo is observable.
    pub(crate) struct TreeFixture {
        entries: Vec<(String, String)>,
        blobs: HashMap<String, Vec<u8>>,
        reads: AtomicUsize,
    }

    fn blob_id(bytes: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        format!("{:x}", hasher.finalize())
    }

    impl TreeFixture {
        pub(crate) fn new() -> Self {
            Self {
                entries: Vec::new(),
                blobs: HashMap::new(),
                reads: AtomicUsize::new(0),
            }
        }

        /// A file whose CONTENT matters (a manifest).
        pub(crate) fn file(mut self, path: &str, content: &str) -> Self {
            let id = blob_id(content.as_bytes());
            self.blobs.insert(id.clone(), content.as_bytes().to_vec());
            self.entries.push((path.to_string(), id));
            self
        }

        /// An ordinary source path whose content is irrelevant to the graph.
        /// `version` distinguishes two trees that differ only in source content.
        pub(crate) fn source(mut self, path: &str, version: &str) -> Self {
            self.entries.push((
                path.to_string(),
                blob_id(format!("{path}:{version}").as_bytes()),
            ));
            self
        }

        pub(crate) fn entries(&self) -> Vec<(String, String)> {
            let mut entries = self.entries.clone();
            entries.sort();
            entries
        }

        pub(crate) fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }
    }

    impl BlobReader for TreeFixture {
        fn read(&self, ids: &[&str]) -> Result<HashMap<String, Vec<u8>>, String> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(ids
                .iter()
                .filter_map(|id| {
                    self.blobs
                        .get(*id)
                        .map(|bytes| ((*id).to_string(), bytes.clone()))
                })
                .collect())
        }
    }

    pub(crate) fn repo_root() -> PathBuf {
        // src-tauri/os/cairn-core -> repo root
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(3)
            .expect("cairn-core sits three levels under the repo root")
            .to_path_buf()
    }

    /// This repository's REAL manifests as a sealed-tree fixture. The graph the
    /// engine derives from it is the graph it derives in production, so the
    /// assertions below are about the actual workspace rather than a model of it.
    pub(crate) fn real_workspace() -> TreeFixture {
        let root = repo_root();
        let mut paths: Vec<String> = vec![
            "src-tauri/Cargo.toml".to_string(),
            "package.json".to_string(),
            "tsconfig.json".to_string(),
            "tsconfig.node.json".to_string(),
        ];
        let workspace: toml::Value = std::fs::read_to_string(root.join("src-tauri/Cargo.toml"))
            .expect("the workspace manifest is readable")
            .parse()
            .expect("the workspace manifest parses");
        for member in workspace["workspace"]["members"]
            .as_array()
            .expect("members is an array")
        {
            let member = member.as_str().expect("a member is a string");
            if member == "." {
                continue;
            }
            paths.push(format!("src-tauri/{member}/Cargo.toml"));
        }
        for workspace_dir in ["web"].into_iter().map(PathBuf::from).chain(
            std::fs::read_dir(root.join("packages"))
                .expect("packages/ exists")
                .filter_map(Result::ok)
                .map(|entry| PathBuf::from("packages").join(entry.file_name())),
        ) {
            for name in ["package.json", "tsconfig.json"] {
                paths.push(
                    workspace_dir
                        .join(name)
                        .to_string_lossy()
                        .replace('\\', "/"),
                );
            }
        }

        let mut fixture = TreeFixture::new();
        for path in paths {
            if let Ok(content) = std::fs::read_to_string(root.join(&path)) {
                fixture = fixture.file(&path, &content);
            }
        }
        fixture
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::{real_workspace, TreeFixture};
    use super::*;
    use crate::config::project_settings::{CheckPolicy, CheckResourceClass, CheckWhen};

    fn check(command: &str, impact: Option<&[&str]>, scope: Option<&[&str]>) -> CheckCommand {
        CheckCommand {
            command: command.to_string(),
            impact: impact.map(|globs| globs.iter().map(|g| g.to_string()).collect()),
            scope: scope.map(|tokens| {
                CheckScopeSelector::Many(tokens.iter().map(|t| t.to_string()).collect())
            }),
            policy: CheckPolicy::Advisory,
            when: CheckWhen::Write,
            resource_class: CheckResourceClass::Shared,
            timeout: None,
            executor: None,
            verdict_environment: Vec::new(),
            verdict_platforms: None,
            fixes: false,
        }
    }

    fn extra(pairs: &[(&str, &[&str])]) -> HashMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(node, globs)| {
                (
                    (*node).to_string(),
                    globs.iter().map(|g| (*g).to_string()).collect(),
                )
            })
            .collect()
    }

    fn resolve(
        check: &CheckCommand,
        extra_inputs: &HashMap<String, Vec<String>>,
        fixture: &TreeFixture,
    ) -> InputSelector {
        let entries = fixture.entries();
        let snapshot = TreeSnapshot::new(Some(&entries), fixture);
        resolve_one(check, extra_inputs, &snapshot)
    }

    fn graph_of(fixture: &TreeFixture) -> Arc<ProjectGraph> {
        let entries = fixture.entries();
        let snapshot = TreeSnapshot::new(Some(&entries), fixture);
        project_graph(&snapshot).expect("the fixture derives a graph")
    }

    /// The domain graph, asserting it derived. A domain that failed is absent
    /// rather than partial, so this is also how a test says "this fixture is
    /// well-formed enough that the assertions below are about edges, not about
    /// derivation giving up".
    fn domain_of(fixture: &TreeFixture, domain: Domain) -> Arc<DomainGraph> {
        graph_of(fixture)
            .domain(domain)
            .map(Arc::clone)
            .unwrap_or_else(|| panic!("the fixture derives its {:?} domain", domain))
    }

    fn names(set: &BTreeSet<String>) -> Vec<&str> {
        set.iter().map(String::as_str).collect()
    }

    fn seed(name: &str) -> BTreeSet<String> {
        BTreeSet::from([name.to_string()])
    }

    // --- graph derivation against the real workspace -----------------------

    #[test]
    fn the_real_rust_workspace_derives_its_own_edges() {
        let fixture = real_workspace();
        let rust = domain_of(&fixture, Domain::Rust);

        assert_eq!(
            rust.nodes.len(),
            16,
            "every declared workspace member becomes a node: {:?}",
            rust.nodes.keys().collect::<Vec<_>>()
        );
        // A path dependency reachable ONLY through cairn-symbols. A hand-written
        // glob list has no way to know this edge exists.
        assert!(rust.nodes["cairn-symbols"]
            .deps
            .contains("ast-grep-outline"));
        // cairn-core dev-depends on itself; a self-edge is not an edge.
        assert!(!rust.nodes["cairn-core"].deps.contains("cairn-core"));

        // If this list changes, a real dependency edge moved — update it
        // deliberately rather than loosening the assertion.
        assert_eq!(
            names(&rust.forward_closure(&seed("cairn-core"))),
            vec![
                "ast-grep-outline",
                "cairn-analytics",
                "cairn-codec",
                "cairn-common",
                "cairn-core",
                "cairn-db",
                "cairn-executor",
                "cairn-sandbox",
                "cairn-symbols",
                "cairn-vcs",
                "cairn-worktree",
            ]
        );
        assert_eq!(
            names(&rust.forward_closure(&seed("cairn-cmd"))),
            vec!["cairn-cmd", "cairn-common", "cairn-worktree"]
        );
    }

    #[test]
    fn a_rust_path_belongs_to_its_longest_prefix_member() {
        let fixture = real_workspace();
        let rust = domain_of(&fixture, Domain::Rust);
        // `src-tauri/` is the app crate's own manifest directory AND the parent
        // of every other member, so a shorter-prefix rule would give it the
        // whole workspace.
        assert_eq!(
            rust.owner("src-tauri/os/cairn-core/src/lib.rs"),
            Some("cairn-core")
        );
        assert_eq!(rust.owner("src-tauri/src/main.rs"), Some("cairn"));
        assert_eq!(rust.owner("docs/checks.md"), None);
    }

    #[test]
    fn the_real_ts_workspaces_include_the_alias_edge() {
        let fixture = real_workspace();
        let ts = domain_of(&fixture, Domain::Ts);
        for name in [
            "cairn",
            "@cairn/ui",
            "@cairn/sdk",
            "@cairn/harness",
            "cairn-web",
        ] {
            assert!(ts.nodes.contains_key(name), "{name} is a workspace");
        }
        assert!(ts.nodes["@cairn/harness"].deps.contains("@cairn/sdk"));
        assert!(ts.nodes["cairn-web"].deps.contains("@cairn/ui"));
        assert!(ts.nodes["cairn"].deps.contains("@cairn/ui"));
        // The root package's directory is the repo root, so its roots come from
        // the root tsconfig's `include` instead.
        assert_eq!(ts.owner("src/App.tsx"), Some("cairn"));
        assert_eq!(ts.owner("packages/ui/src/Button.tsx"), Some("@cairn/ui"));
        assert_eq!(ts.owner("docs/checks.md"), None);
    }

    // --- graph derivation: generality and degradation ----------------------

    const WORKSPACE_MANIFEST: &str = r#"
[workspace]
members = [".", "os/leaf", "os/mid"]
[package]
name = "app"
[dependencies]
mid = { path = "os/mid", package = "cairn-mid" }
"#;

    fn three_crate_workspace() -> TreeFixture {
        TreeFixture::new()
            .file("src-tauri/Cargo.toml", WORKSPACE_MANIFEST)
            // A manifest that is NOT a declared member. Globbing for Cargo.toml
            // would pick it up; the member list does not.
            .file("src-tauri/os/Cargo.toml", "[workspace]\nmembers = []\n")
            .file(
                "src-tauri/os/leaf/Cargo.toml",
                "[package]\nname = \"leaf\"\n",
            )
            .file(
                "src-tauri/os/mid/Cargo.toml",
                "[package]\nname = \"cairn-mid\"\n\
                 [target.'cfg(unix)'.dependencies]\n\
                 leaf = { path = \"../leaf\" }\n\
                 libc = \"0.2\"\n",
            )
            .source("src-tauri/os/leaf/src/lib.rs", "v1")
            .source("src-tauri/os/mid/src/lib.rs", "v1")
            .source("src-tauri/src/main.rs", "v1")
    }

    #[test]
    fn a_target_cfg_dependency_table_is_a_real_edge() {
        let rust = domain_of(&three_crate_workspace(), Domain::Rust);
        assert_eq!(
            names(&rust.forward_closure(&seed("cairn-mid"))),
            vec!["cairn-mid", "leaf"],
            "a cfg-gated path dependency is an input on the platforms checks run on"
        );
        // A renamed dependency resolves through `package = "..."`.
        assert!(rust.nodes["app"].deps.contains("cairn-mid"));
    }

    #[test]
    fn a_non_member_manifest_is_not_a_member() {
        let rust = domain_of(&three_crate_workspace(), Domain::Rust);
        assert_eq!(
            names(&rust.nodes.keys().cloned().collect()),
            vec!["app", "cairn-mid", "leaf"]
        );
    }

    #[test]
    fn an_unparseable_workspace_manifest_degrades_to_the_whole_tree() {
        let fixture = TreeFixture::new()
            .file("src-tauri/Cargo.toml", "[workspace\nmembers = broken")
            .source("src-tauri/os/cairn-core/src/lib.rs", "v1");
        let selector = resolve(
            &check("cargo test", None, Some(&["rust:cairn-core"])),
            &HashMap::new(),
            &fixture,
        );
        assert!(!selector.narrows(), "an underivable closure cannot narrow");
        assert!(selector.is_declared());
        assert!(selector.matches("anything/at/all"));
    }

    #[test]
    fn an_unknown_scope_token_degrades_to_the_whole_tree() {
        let fixture = real_workspace();
        for tokens in [
            vec!["rust:no-such-crate"],
            vec!["nonsense"],
            // A scope spanning two domains has no single ownership rule.
            vec!["rust:cairn-core", "ts:cairn"],
        ] {
            let selector = resolve(
                &check("cargo test", None, Some(&tokens)),
                &HashMap::new(),
                &fixture,
            );
            assert!(!selector.narrows(), "{tokens:?} must not narrow");
            assert!(selector.matches("docs/checks.md"));
        }
    }

    #[test]
    fn graph_derivation_is_memoized_on_the_manifests_alone() {
        // Two trees whose manifests are byte-identical and whose SOURCE differs.
        // Derivation must read blobs once across both: planning cost scales with
        // manifest states, never with commits.
        let first = three_crate_workspace();
        let second = TreeFixture::new()
            .file("src-tauri/Cargo.toml", WORKSPACE_MANIFEST)
            .file("src-tauri/os/Cargo.toml", "[workspace]\nmembers = []\n")
            .file(
                "src-tauri/os/leaf/Cargo.toml",
                "[package]\nname = \"leaf\"\n",
            )
            .file(
                "src-tauri/os/mid/Cargo.toml",
                "[package]\nname = \"cairn-mid\"\n\
                 [target.'cfg(unix)'.dependencies]\n\
                 leaf = { path = \"../leaf\" }\n\
                 libc = \"0.2\"\n",
            )
            .source("src-tauri/os/leaf/src/lib.rs", "v2")
            .source("src-tauri/os/mid/src/lib.rs", "v2")
            .source("src-tauri/src/main.rs", "v2");

        // Ten checks over one tree cost one derivation for the whole planning
        // pass, not one per check.
        let checks: HashMap<String, CheckCommand> = (0..10)
            .map(|index| {
                (
                    format!("check-{index}"),
                    check("cargo test", None, Some(&["rust:cairn-mid"])),
                )
            })
            .collect();
        let entries = first.entries();
        let snapshot = TreeSnapshot::new(Some(&entries), &first);
        let resolved = ResolvedInputs::resolve(&checks, &HashMap::new(), &snapshot);
        assert!(resolved.for_check("check-0").narrows());
        assert!(
            first.reads() <= 1,
            "a ten-check contract over one tree derives the graph once, not ten times"
        );

        // Reuse across trees, against a memo this test owns.
        let memo: GraphMemo = Mutex::new(Vec::new());
        let second_entries = second.entries();
        assert!(project_graph_memoized(&snapshot, &memo).is_some());
        let after_first = first.reads();
        assert!(
            project_graph_memoized(&TreeSnapshot::new(Some(&second_entries), &second), &memo)
                .is_some()
        );
        assert_eq!(
            second.reads(),
            0,
            "a tree whose manifests are unchanged reuses the memoized graph"
        );
        assert_eq!(
            first.reads(),
            after_first,
            "and derives nothing further for either tree"
        );
    }

    // --- acceptance: selection ---------------------------------------------

    #[test]
    fn a_frontend_change_is_no_rust_check_s_input() {
        let fixture = real_workspace();
        let selector = resolve(
            &check("cargo test", None, Some(&["rust:cairn-core"])),
            &HashMap::new(),
            &fixture,
        );
        assert!(!selector.matches("src/App.tsx"));
        assert!(!selector.matches("packages/ui/src/Button.tsx"));
        assert!(!selector.matches("docs/checks.md"));
    }

    #[test]
    fn a_dependency_s_change_is_an_input_of_its_dependents_only() {
        let fixture = real_workspace();
        let core = resolve(
            &check("cargo test", None, Some(&["rust:cairn-core"])),
            &HashMap::new(),
            &fixture,
        );
        let cmd = resolve(
            &check("cargo test", None, Some(&["rust:cairn-cmd"])),
            &HashMap::new(),
            &fixture,
        );
        let vcs = "src-tauri/os/cairn-vcs/src/lib.rs";
        assert!(core.matches(vcs), "cairn-core compiles against cairn-vcs");
        assert!(!cmd.matches(vcs), "cairn-cmd does not");
        // Both still own their own sources.
        assert!(cmd.matches("src-tauri/os/cairn-cmd/src/main.rs"));
    }

    #[test]
    fn a_workspace_global_is_every_rust_closure_s_input() {
        let fixture = real_workspace();
        for token in ["rust:cairn-core", "rust:cairn-cmd"] {
            let selector = resolve(
                &check("cargo test", None, Some(&[token])),
                &HashMap::new(),
                &fixture,
            );
            for global in [
                "src-tauri/Cargo.lock",
                "src-tauri/Cargo.toml",
                "src-tauri/.cargo/config.toml",
            ] {
                assert!(selector.matches(global), "{global} invalidates {token}");
            }
        }
    }

    #[test]
    fn a_scope_list_unions_its_closures() {
        let fixture = real_workspace();
        let selector = resolve(
            &check(
                "cargo test",
                None,
                Some(&["rust:cairn-core", "rust:cairn-runner"]),
            ),
            &HashMap::new(),
            &fixture,
        );
        // cairn-transport is reachable only through cairn-runner.
        assert!(selector.matches("src-tauri/cairn-transport/src/lib.rs"));
        assert!(selector.matches("src-tauri/os/cairn-vcs/src/lib.rs"));
        assert!(!selector.matches("src-tauri/os/cairn-cmd/src/main.rs"));
    }

    #[test]
    fn a_ui_package_change_reaches_the_root_app_through_the_alias() {
        // The alias mechanism on its own: a root package that declares NO
        // dependency on the UI workspace still compiles against it, because the
        // tsconfig `paths` entry is how the import resolves.
        let alias_only = TreeFixture::new()
            .file(
                "package.json",
                r#"{"name": "app", "workspaces": ["packages/*"]}"#,
            )
            .file(
                "tsconfig.json",
                r#"{
                  // tsconfig permits comments and trailing commas.
                  "compilerOptions": {"paths": {"@ui/*": ["./packages/ui/src/*"],},},
                  "include": ["src"],
                }"#,
            )
            .file("packages/ui/package.json", r#"{"name": "@ui/lib"}"#)
            .source("src/App.tsx", "v1")
            .source("packages/ui/src/Button.tsx", "v1");
        let selector = resolve(
            &check("tsc --noEmit", None, Some(&["ts:app"])),
            &HashMap::new(),
            &alias_only,
        );
        assert!(
            selector.matches("packages/ui/src/Button.tsx"),
            "the tsconfig alias IS the edge"
        );

        // And against the repository's own manifests, where the root package
        // declares the dependency AND aliases it.
        let real = real_workspace();
        let selector = resolve(
            &check("tsc --noEmit", None, Some(&["ts:cairn"])),
            &HashMap::new(),
            &real,
        );
        assert!(selector.matches("packages/ui/src/Button.tsx"));
        assert!(selector.matches("src/App.tsx"));
        assert!(selector.matches("package.json"));
        assert!(selector.matches("bun.lock"));
        assert!(
            !selector.matches("src-tauri/os/cairn-core/src/lib.rs"),
            "a Rust change is not a typecheck input"
        );
    }

    // --- partial derivation is never allowed to look like a small closure ---

    /// The three-crate workspace with one member's manifest replaced. The scoped
    /// crate stays valid in every case below; what breaks is something it
    /// DEPENDS on, which is the direction that silently narrows.
    fn workspace_with_member(path: &str, manifest: Option<&str>) -> TreeFixture {
        let mut fixture = TreeFixture::new()
            .file("src-tauri/Cargo.toml", WORKSPACE_MANIFEST)
            .file(
                "src-tauri/os/mid/Cargo.toml",
                "[package]\nname = \"cairn-mid\"\n\
                 [dependencies]\n\
                 leaf = { path = \"../leaf\" }\n",
            )
            .source("src-tauri/os/leaf/src/lib.rs", "v1")
            .source("src-tauri/os/mid/src/lib.rs", "v1");
        if let Some(manifest) = manifest {
            fixture = fixture.file(path, manifest);
        }
        fixture
    }

    #[test]
    fn a_dependency_whose_manifest_will_not_parse_fails_the_whole_rust_domain() {
        // Before this was enforced, `leaf` simply vanished from the member set:
        // the cairn-mid -> leaf edge was filtered out as external, leaf's
        // directory fell to the app crate by prefix, and a check scoped to
        // cairn-mid reused a verdict that never examined leaf's sources.
        for broken in [
            Some("[package\nname = broken"),
            // A manifest that parses but names no package is equally unusable.
            Some("[dependencies]\nlibc = \"0.2\"\n"),
            // And a member listed in the workspace but absent from the tree.
            None,
        ] {
            let fixture = workspace_with_member("src-tauri/os/leaf/Cargo.toml", broken);
            let selector = resolve(
                &check("cargo test", None, Some(&["rust:cairn-mid"])),
                &HashMap::new(),
                &fixture,
            );
            assert!(
                !selector.narrows(),
                "a graph missing a member cannot narrow anything"
            );
            assert!(
                selector.matches("src-tauri/os/leaf/src/lib.rs"),
                "the dependency's sources stay inputs"
            );
            assert!(selector.is_declared(), "the declaration still keys");
        }
    }

    #[test]
    fn a_broken_manifest_fails_only_its_own_domain() {
        // Domains are independent: a Rust workspace nobody can read says nothing
        // about whether the TypeScript closure is trustworthy.
        let fixture =
            workspace_with_member("src-tauri/os/leaf/Cargo.toml", Some("[package\nbroken"))
                .file(
                    "package.json",
                    r#"{"name": "app", "workspaces": ["packages/*"]}"#,
                )
                .file("tsconfig.json", r#"{"include": ["src"]}"#)
                .file("packages/ui/package.json", r#"{"name": "@ui/lib"}"#)
                .source("src/App.tsx", "v1")
                .source("packages/ui/src/Button.tsx", "v1");

        let ts = resolve(
            &check("tsc --noEmit", None, Some(&["ts:@ui/lib"])),
            &HashMap::new(),
            &fixture,
        );
        assert!(ts.narrows(), "the TypeScript graph derived fine");
        assert!(!ts.matches("src/App.tsx"));

        let rust = resolve(
            &check("cargo test", None, Some(&["rust:cairn-mid"])),
            &HashMap::new(),
            &fixture,
        );
        assert!(!rust.narrows(), "the Rust graph did not");
    }

    #[test]
    fn an_unparseable_tsconfig_fails_the_ts_domain() {
        // A tsconfig that will not parse means unknown alias edges, and an alias
        // is the only thing tying `@ui/lib` to the root app here. Reading zero
        // edges out of it would key the root app on a closure of one.
        let fixture = TreeFixture::new()
            .file(
                "package.json",
                r#"{"name": "app", "workspaces": ["packages/*"]}"#,
            )
            .file("tsconfig.json", r#"{"include": ["src"]}"#)
            .file("packages/ui/package.json", r#"{"name": "@ui/lib"}"#)
            .file("packages/ui/tsconfig.json", "{ not json at all ")
            .source("src/App.tsx", "v1")
            .source("packages/ui/src/Button.tsx", "v1");
        let selector = resolve(
            &check("tsc --noEmit", None, Some(&["ts:app"])),
            &HashMap::new(),
            &fixture,
        );
        assert!(!selector.narrows());
        assert!(selector.matches("packages/ui/src/Button.tsx"));
    }

    #[test]
    fn a_workspace_package_without_a_name_fails_the_ts_domain() {
        // It still OWNS its directory, so excluding it would hand its files to
        // the root app by prefix and key them under the wrong closure.
        let fixture = TreeFixture::new()
            .file(
                "package.json",
                r#"{"name": "app", "workspaces": ["packages/*"]}"#,
            )
            .file("tsconfig.json", r#"{"include": ["src"]}"#)
            .file("packages/ui/package.json", r#"{"private": true}"#)
            .source("packages/ui/src/Button.tsx", "v1");
        let selector = resolve(
            &check("tsc --noEmit", None, Some(&["ts:app"])),
            &HashMap::new(),
            &fixture,
        );
        assert!(!selector.narrows());
        assert!(selector.matches("packages/ui/src/Button.tsx"));
    }

    #[test]
    fn a_workspace_declaration_that_does_not_read_in_full_fails_the_ts_domain() {
        // `workspaces` decides which package manifests become nodes at all, so
        // keeping the valid subset of a declaration would drop a real workspace
        // and filter every edge into it as external.
        let ui_alias = r#"{"compilerOptions": {"paths": {"@ui/*": ["./packages/ui/src/*"]}},
                          "include": ["src"]}"#;
        for workspaces in [
            // One valid pattern beside one that will not compile.
            r#"["packages/*", "packages/["]"#,
            // One valid pattern beside a non-string entry.
            r#"["packages/*", 42]"#,
            // A declaration shape this does not understand.
            r#""packages/*""#,
            r#"{"packages": "packages/*"}"#,
        ] {
            let fixture = TreeFixture::new()
                .file(
                    "package.json",
                    &format!(r#"{{"name": "app", "workspaces": {workspaces}}}"#),
                )
                .file("tsconfig.json", ui_alias)
                .file("packages/ui/package.json", r#"{"name": "@ui/lib"}"#)
                .source("src/App.tsx", "v1")
                .source("packages/ui/src/Button.tsx", "v1");
            let selector = resolve(
                &check("tsc --noEmit", None, Some(&["ts:app"])),
                &HashMap::new(),
                &fixture,
            );
            assert!(
                !selector.narrows(),
                "{workspaces} must not retain its valid subset"
            );
            assert!(selector.matches("packages/ui/src/Button.tsx"));
        }

        // The legitimate cases stay legitimate: a project with no workspaces at
        // all still derives, as a root node that owns its tsconfig `include`.
        for workspaces in ["", r#", "workspaces": []"#, r#", "workspaces": {}"#] {
            let fixture = TreeFixture::new()
                .file("package.json", &format!(r#"{{"name": "app"{workspaces}}}"#))
                .file("tsconfig.json", r#"{"include": ["src"]}"#)
                .source("src/App.tsx", "v1")
                .source("docs/notes.md", "v1");
            let selector = resolve(
                &check("tsc --noEmit", None, Some(&["ts:app"])),
                &HashMap::new(),
                &fixture,
            );
            assert!(selector.narrows(), "a root-only project is derivable");
            assert!(selector.matches("src/App.tsx"));
            assert!(!selector.matches("docs/notes.md"));
        }
    }

    #[test]
    fn a_manifest_blob_the_object_database_lacks_fails_every_domain() {
        // The read is best-effort by shape: an id it cannot serve comes back
        // absent rather than as an error, so derivation has to notice.
        let fixture = three_crate_workspace();
        let mut entries = fixture.entries();
        entries.push((
            "packages/ui/package.json".to_string(),
            "0000000000000000000000000000000000000000".to_string(),
        ));
        entries.sort();
        let snapshot = TreeSnapshot::new(Some(&entries), &fixture);
        assert!(
            project_graph(&snapshot).is_none(),
            "a manifest that will not materialize is not a manifest we can skip"
        );
    }

    // --- extra inputs ------------------------------------------------------

    #[test]
    fn a_node_s_extra_inputs_compose_to_every_check_that_reaches_it() {
        let fixture = real_workspace();
        let extra_inputs = extra(&[(
            "rust:cairn-db",
            &[
                "src-tauri/turso_migrations/**",
                "src-tauri/turso_migrations_team/**",
            ],
        )]);
        let migration = "src-tauri/turso_migrations/0084_x.sql";

        let core = resolve(
            &check("cargo test", None, Some(&["rust:cairn-core"])),
            &extra_inputs,
            &fixture,
        );
        assert!(
            core.matches(migration),
            "cairn-core's tests compile cairn-db's migrations in"
        );
        let cmd = resolve(
            &check("cargo test", None, Some(&["rust:cairn-cmd"])),
            &extra_inputs,
            &fixture,
        );
        assert!(!cmd.matches(migration), "cairn-cmd never reaches cairn-db");
    }

    // --- the definition that enters the cache key --------------------------

    #[test]
    fn the_definition_names_the_resolved_closure_and_extra_inputs() {
        let fixture = real_workspace();
        let core = check("cargo test", None, Some(&["rust:cairn-core"]));
        let plain = resolve(&core, &HashMap::new(), &fixture);
        assert!(plain
            .definition()
            .contains(&"scope:rust:cairn-core".to_string()));
        assert!(plain
            .definition()
            .contains(&"closure:cairn-vcs".to_string()));

        // Retargeting the scope changes the definition.
        let cmd = resolve(
            &check("cargo test", None, Some(&["rust:cairn-cmd"])),
            &HashMap::new(),
            &fixture,
        );
        assert_ne!(plain.definition(), cmd.definition());

        // So does an extra-input declaration the closure reaches — even though
        // no file changed and the closure membership is identical.
        let with_extra = resolve(
            &core,
            &extra(&[("rust:cairn-db", &["src-tauri/turso_migrations/**"])]),
            &fixture,
        );
        assert_ne!(plain.definition(), with_extra.definition());
    }

    #[test]
    fn a_glob_selector_keys_on_its_globs_alone() {
        let selector = InputSelector::from_globs(&["b/**".to_string(), "a/**".to_string()]);
        assert_eq!(
            selector.definition(),
            ["a/**".to_string(), "b/**".to_string()]
        );
        assert!(selector.narrows());
        assert!(!selector.keys_on_whole_tree());
    }

    // --- declaration errors -------------------------------------------------

    #[test]
    fn declaring_both_impact_and_scope_is_a_configuration_error() {
        let fixture = real_workspace();
        let selector = resolve(
            &check(
                "cargo test",
                Some(&["src-tauri/**"]),
                Some(&["rust:cairn-core"]),
            ),
            &HashMap::new(),
            &fixture,
        );
        let error = selector
            .config_error()
            .expect("two definitions of one check's inputs cannot both hold");
        assert!(error.contains("impact"));
        assert!(error.contains("scope"));
        assert!(!selector.narrows());
    }

    #[test]
    fn a_check_declaring_nothing_keys_on_the_whole_tree() {
        let selector = InputSelector::everything();
        assert!(selector.keys_on_whole_tree());
        assert!(!selector.is_declared());
        assert!(selector.matches("anything"));
        assert!(selector.definition().is_empty());
    }

    #[test]
    fn a_declared_but_unresolvable_selector_is_not_mistaken_for_no_declaration() {
        // The distinction matters: "nothing declared" applies to every change and
        // keys on the whole-tree HASH, while "declared but unresolvable" applies
        // to every change and keys on the whole entry LIST plus its definition,
        // so the declaration still moves the key.
        let selector = InputSelector::from_globs(&["src-tauri/[".to_string()]);
        assert!(selector.is_declared());
        assert!(!selector.keys_on_whole_tree());
        assert!(!selector.narrows());
        assert!(selector.matches("anything"));
    }

    #[test]
    fn the_fetch_gate_fires_for_scope_as_well_as_impact() {
        let none = check("cargo test", None, None);
        assert!(!any_check_declares_inputs([&none]));
        assert!(any_check_declares_inputs([&check(
            "cargo test",
            Some(&["src/**"]),
            None
        )]));
        assert!(any_check_declares_inputs([&check(
            "cargo test",
            None,
            Some(&["rust:cairn-core"])
        )]));
    }
}
