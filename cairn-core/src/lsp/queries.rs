//! Name-to-position resolution and op execution.
//!
//! A user asks for an op (`definition`, `references`, ...) against a symbol
//! *name*, optionally container-qualified (`Foo::bar`, `module.func`). The chain
//! is: split on the entry's `container_separator` → `workspace/symbol` lookup →
//! filter to exact-name candidates → resolve to a concrete `(uri, position)` →
//! issue the position-based op request.
//!
//! **Disambiguation is honest:** a bare name resolving to more than one distinct
//! symbol returns the candidate list and issues no op request; zero matches is a
//! clean miss; exactly one proceeds. Capability gating short-circuits an op the
//! server does not advertise to [`QueryOutcome::Unsupported`].

use std::path::Path;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use super::client::{uri_to_path, LspClient, DEFAULT_TIMEOUT, PROJECT_TIMEOUT};
use super::{LspError, LspOp};

/// How long to wait for indexing readiness before answering best-effort.
pub const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// Overall budget for resolving a name to a symbol while the index warms.
const RESOLVE_DEADLINE: Duration = Duration::from_secs(30);
/// Per-poll block waiting for the server to reach quiescence.
const RESOLVE_POLL_SLICE: Duration = Duration::from_millis(750);
/// Pause between resolution polls.
const RESOLVE_POLL_SLEEP: Duration = Duration::from_millis(150);
/// Continuous quiescence required before an empty result is trusted as a
/// genuine miss rather than an indexing lag. A server's progress sequences have
/// brief sub-second gaps that momentarily look quiescent mid-index; only a
/// sustained quiet window means indexing has actually settled.
const MISS_SETTLE: Duration = Duration::from_secs(2);

/// One ambiguous symbol candidate (a bare name matched more than one symbol).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub name: String,
    pub container: Option<String>,
    // Symbol-kind code; retained for the Phase-4 typed candidate surface.
    #[allow(dead_code)]
    pub kind: i64,
    /// Source URI of the symbol's declaration.
    pub uri: String,
    /// Worktree-relative display path.
    pub path: String,
    /// 0-based LSP line of the symbol's selection range.
    pub line: u32,
    /// 0-based LSP character of the symbol's selection range.
    pub character: u32,
}

impl Candidate {
    /// 1-based line for display (grep-style).
    pub fn display_line(&self) -> u32 {
        self.line + 1
    }
}

/// One location result (a grep-style `path:line:snippet` row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationHit {
    pub path: String,
    /// 1-based line for display.
    pub line: u32,
    pub snippet: String,
}

/// A resolved `(uri, (line, character))` position, or `None` for a miss.
type ResolvedPosition = Option<(String, (u32, u32))>;

/// The outcome of a query, ready for [`super::render`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryOutcome {
    Locations(Vec<LocationHit>),
    Hover(String),
    Candidates(Vec<Candidate>),
    Miss,
    Unsupported,
}

/// Resolve `name` to a position and run `op`, returning the outcome and whether
/// the index was still building (best-effort answer when readiness timed out).
pub fn run_named_query(
    client: &LspClient,
    root: &Path,
    op: LspOp,
    name: &str,
    separator: &str,
) -> Result<(QueryOutcome, bool), LspError> {
    if !client.supports(op) {
        return Ok((QueryOutcome::Unsupported, false));
    }

    let (leaf, container) = split_name(name, separator);
    let (candidates, still_indexing) =
        resolve_candidates(client, root, &leaf, container.as_deref())?;

    match candidates.len() {
        0 => Ok((QueryOutcome::Miss, still_indexing)),
        1 => {
            let c = &candidates[0];
            let outcome = execute_op_at(client, root, op, &c.uri, (c.line, c.character))?;
            Ok((outcome, still_indexing))
        }
        _ => Ok((QueryOutcome::Candidates(candidates), still_indexing)),
    }
}

/// Resolve a (possibly container-qualified) name to its candidate symbols via
/// `workspace/symbol`, polling while the index warms.
///
/// A single `workspace/symbol` call during indexing returns nothing, so this
/// polls: it blocks briefly for the server to reach quiescence, queries, and
/// returns as soon as candidates appear. When the server settles (quiescent
/// across [`MISS_CONFIRMATIONS`] consecutive polls) with still no match, it is a
/// genuine miss and returns fast. Exhausting [`RESOLVE_DEADLINE`] returns the
/// empty set flagged `still_indexing`. Returns `(candidates, still_indexing)`.
fn resolve_candidates(
    client: &LspClient,
    root: &Path,
    leaf: &str,
    container: Option<&str>,
) -> Result<(Vec<Candidate>, bool), LspError> {
    let deadline = Instant::now() + RESOLVE_DEADLINE;
    let mut quiescent_since: Option<Instant> = None;
    loop {
        // Pace the loop: block (bounded) for the server to reach quiescence.
        let quiescent = client.wait_ready(RESOLVE_POLL_SLICE);
        let symbols = workspace_symbol(client, leaf)?;
        let candidates = filter_candidates(&symbols, root, leaf, container);
        if !candidates.is_empty() {
            // Found: still_indexing if the server has not fully settled yet.
            return Ok((candidates, !quiescent));
        }
        let now = Instant::now();
        if quiescent {
            // Trust an empty result only after quiescence has held continuously
            // for MISS_SETTLE — long enough to rule out a between-sequence gap.
            let since = *quiescent_since.get_or_insert(now);
            if now.duration_since(since) >= MISS_SETTLE {
                return Ok((Vec::new(), false));
            }
        } else {
            // Server went active again; reset the settle window.
            quiescent_since = None;
        }
        if now >= deadline {
            return Ok((Vec::new(), true));
        }
        std::thread::sleep(RESOLVE_POLL_SLEEP);
    }
}

/// Direct position path (for Phase 3's position-based callers): run `op` at a
/// concrete document position without name resolution.
pub fn execute_op_at(
    client: &LspClient,
    root: &Path,
    op: LspOp,
    uri: &str,
    position: (u32, u32),
) -> Result<QueryOutcome, LspError> {
    if !client.supports(op) {
        return Ok(QueryOutcome::Unsupported);
    }
    let pos = json!({"line": position.0, "character": position.1});
    let td = json!({"uri": uri});

    match op {
        LspOp::Definition => {
            let r = client.request(
                "textDocument/definition",
                json!({"textDocument": td, "position": pos}),
                DEFAULT_TIMEOUT,
            )?;
            Ok(QueryOutcome::Locations(locations_from(&r, root)))
        }
        LspOp::Implementations => {
            let r = client.request(
                "textDocument/implementation",
                json!({"textDocument": td, "position": pos}),
                DEFAULT_TIMEOUT,
            )?;
            Ok(QueryOutcome::Locations(locations_from(&r, root)))
        }
        LspOp::Hover => {
            let r = client.request(
                "textDocument/hover",
                json!({"textDocument": td, "position": pos}),
                DEFAULT_TIMEOUT,
            )?;
            Ok(hover_outcome(&r))
        }
        LspOp::References => {
            // Project-wide: ensure best-effort readiness before fan-out.
            client.wait_ready(READY_TIMEOUT);
            let r = client.request(
                "textDocument/references",
                json!({
                    "textDocument": td,
                    "position": pos,
                    "context": {"includeDeclaration": true}
                }),
                PROJECT_TIMEOUT,
            )?;
            Ok(QueryOutcome::Locations(locations_from(&r, root)))
        }
        LspOp::Callers => {
            client.wait_ready(READY_TIMEOUT);
            let items = client.request(
                "textDocument/prepareCallHierarchy",
                json!({"textDocument": td, "position": pos}),
                DEFAULT_TIMEOUT,
            )?;
            let mut hits = Vec::new();
            for item in as_array(&items) {
                let incoming = client.request(
                    "callHierarchy/incomingCalls",
                    json!({"item": item}),
                    PROJECT_TIMEOUT,
                )?;
                for call in as_array(&incoming) {
                    if let Some(from) = call.get("from") {
                        if let Some(hit) = hit_from_item(from, root) {
                            hits.push(hit);
                        }
                    }
                }
            }
            Ok(QueryOutcome::Locations(hits))
        }
        LspOp::Subtypes => {
            client.wait_ready(READY_TIMEOUT);
            let items = client.request(
                "textDocument/prepareTypeHierarchy",
                json!({"textDocument": td, "position": pos}),
                DEFAULT_TIMEOUT,
            )?;
            let mut hits = Vec::new();
            for item in as_array(&items) {
                let subs = client.request(
                    "typeHierarchy/subtypes",
                    json!({"item": item}),
                    PROJECT_TIMEOUT,
                )?;
                for sub in as_array(&subs) {
                    if let Some(hit) = hit_from_item(&sub, root) {
                        hits.push(hit);
                    }
                }
            }
            Ok(QueryOutcome::Locations(hits))
        }
        // Rename is a write op: it returns a `WorkspaceEdit`, not locations, so
        // it is driven through `compute_rename` and never reaches this position
        // dispatcher. Degrade honestly rather than panic if it ever does.
        LspOp::Rename => Ok(QueryOutcome::Unsupported),
    }
}

/// The outcome of resolving a bare `old_name` to a single rename position.
/// Disambiguation is honest: a name matching more than one symbol returns the
/// candidate list and issues no rename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenameTarget {
    Resolved { uri: String, position: (u32, u32) },
    Ambiguous(Vec<Candidate>),
    Miss,
}

/// Resolve a (possibly container-qualified) `name` to the position of its single
/// declaration via `workspace/symbol`, for a name-located rename. Reuses the
/// readiness-aware candidate resolution so an ambiguous name refuses to guess.
pub fn resolve_rename_position(
    client: &LspClient,
    root: &Path,
    name: &str,
    separator: &str,
) -> Result<RenameTarget, LspError> {
    let (leaf, container) = split_name(name, separator);
    let (candidates, _still_indexing) =
        resolve_candidates(client, root, &leaf, container.as_deref())?;
    Ok(match candidates.len() {
        0 => RenameTarget::Miss,
        1 => RenameTarget::Resolved {
            uri: candidates[0].uri.clone(),
            position: (candidates[0].line, candidates[0].character),
        },
        _ => RenameTarget::Ambiguous(candidates),
    })
}

/// Compute the `WorkspaceEdit` for renaming the symbol at `position` in `uri` to
/// `new_name`. Capability-gated (an absent `renameProvider` is an honest
/// `Unsupported`, not a hang); waits on indexing readiness like other
/// project-wide ops; and, when the server advertises it, probes
/// `textDocument/prepareRename` first so a non-renameable element (keyword,
/// macro expansion) fails with the server's reason rather than a partial edit.
/// Returns the raw `WorkspaceEdit` value for the orchestrator to translate.
pub fn compute_rename(
    client: &LspClient,
    uri: &str,
    position: (u32, u32),
    new_name: &str,
) -> Result<Value, LspError> {
    if !client.supports(LspOp::Rename) {
        return Err(LspError::Unsupported {
            op: LspOp::Rename,
            language: client.language().to_string(),
        });
    }
    client.wait_ready(READY_TIMEOUT);
    if client.rename_prepare_supported() {
        let prep = client.request(
            "textDocument/prepareRename",
            json!({
                "textDocument": {"uri": uri},
                "position": {"line": position.0, "character": position.1}
            }),
            DEFAULT_TIMEOUT,
        )?;
        if prep.is_null() {
            return Err(LspError::Transport(
                "the element at this position cannot be renamed".to_string(),
            ));
        }
    }
    client.request(
        "textDocument/rename",
        json!({
            "textDocument": {"uri": uri},
            "position": {"line": position.0, "character": position.1},
            "newName": new_name
        }),
        PROJECT_TIMEOUT,
    )
}

/// Pure `workspace/symbol` discovery: resolve `query` to its exact-name
/// candidates via the readiness-aware lookup and return them as
/// [`QueryOutcome::Candidates`], issuing no op. Powers the node-scoped `?search=`
/// discovery entry point.
pub fn search_symbols(
    client: &LspClient,
    root: &Path,
    query: &str,
) -> Result<QueryOutcome, LspError> {
    let (candidates, _still_indexing) = resolve_candidates(client, root, query, None)?;
    Ok(QueryOutcome::Candidates(candidates))
}

/// Resolve `name` to a position within a single file via
/// `textDocument/documentSymbol` — local, index-independent resolution for the
/// file-scoped projection. Returns the `(uri, position)` of the first exact-name
/// match (respecting a container qualifier when present), or `None` for a miss.
pub fn resolve_in_file(
    client: &LspClient,
    root: &Path,
    file_uri: &str,
    name: &str,
    separator: &str,
) -> Result<ResolvedPosition, LspError> {
    let _ = root;
    let (leaf, container) = split_name(name, separator);
    let symbols = client.request(
        "textDocument/documentSymbol",
        json!({"textDocument": {"uri": file_uri}}),
        DEFAULT_TIMEOUT,
    )?;
    let mut found: Option<(u32, u32)> = None;
    find_document_symbol(&as_array(&symbols), &leaf, container.as_deref(), &mut found);
    Ok(found.map(|pos| (file_uri.to_string(), pos)))
}

/// Recursively search a `documentSymbol` response — hierarchical `DocumentSymbol`
/// (with `selectionRange`/`children`) or flat `SymbolInformation` (with
/// `location.range`) — for an exact-name match, writing the first hit's position.
fn find_document_symbol(
    symbols: &[Value],
    leaf: &str,
    container: Option<&str>,
    found: &mut Option<(u32, u32)>,
) {
    for sym in symbols {
        if found.is_some() {
            return;
        }
        let name = sym.get("name").and_then(|n| n.as_str()).unwrap_or("");
        if name == leaf {
            // Hierarchical DocumentSymbol: selectionRange points at the name.
            // Flat SymbolInformation: location.range.
            let pos = sym.get("selectionRange").and_then(range_start).or_else(|| {
                sym.get("location")
                    .and_then(|location| location.get("range"))
                    .and_then(range_start)
            });
            if let Some(pos) = pos {
                let container_name = sym
                    .get("containerName")
                    .and_then(|c| c.as_str())
                    .filter(|c| !c.is_empty());
                if container.is_none() || container == container_name {
                    *found = Some(pos);
                    return;
                }
            }
        }
        if let Some(children) = sym.get("children").and_then(|c| c.as_array()) {
            find_document_symbol(children, leaf, container, found);
        }
    }
}

fn workspace_symbol(client: &LspClient, query: &str) -> Result<Vec<Value>, LspError> {
    let r = client.request("workspace/symbol", json!({"query": query}), DEFAULT_TIMEOUT)?;
    Ok(as_array(&r))
}

/// Split a possibly container-qualified name on `separator` into `(leaf,
/// container)`. `Foo::bar` with `"::"` → `("bar", Some("Foo"))`; an unqualified
/// name → `(name, None)`.
fn split_name(name: &str, separator: &str) -> (String, Option<String>) {
    if !separator.is_empty() {
        if let Some(idx) = name.rfind(separator) {
            let container = &name[..idx];
            let leaf = &name[idx + separator.len()..];
            if !leaf.is_empty() {
                let container = if container.is_empty() {
                    None
                } else {
                    Some(container.to_string())
                };
                return (leaf.to_string(), container);
            }
        }
    }
    (name.to_string(), None)
}

/// Keep symbols whose name equals `leaf` exactly, and (when a container was
/// requested) whose `containerName` matches it. Each surviving symbol becomes a
/// [`Candidate`] anchored at its selection-range start.
fn filter_candidates(
    symbols: &[Value],
    root: &Path,
    leaf: &str,
    container: Option<&str>,
) -> Vec<Candidate> {
    let mut out = Vec::new();
    for sym in symbols {
        let name = sym.get("name").and_then(|n| n.as_str()).unwrap_or("");
        if name != leaf {
            continue;
        }
        let container_name = sym
            .get("containerName")
            .and_then(|c| c.as_str())
            .filter(|c| !c.is_empty())
            .map(str::to_string);
        if let Some(want) = container {
            if container_name.as_deref() != Some(want) {
                continue;
            }
        }
        let kind = sym.get("kind").and_then(|k| k.as_i64()).unwrap_or(0);
        // location is OneOf<Location, WorkspaceLocation>: both carry `uri`;
        // `range` (and thus a position) is present for the Location variant.
        let Some(location) = sym.get("location") else {
            continue;
        };
        let Some(uri) = location.get("uri").and_then(|u| u.as_str()) else {
            continue;
        };
        let (line, character) = location
            .get("range")
            .and_then(range_start)
            .unwrap_or((0, 0));
        let path = display_path(uri, root);
        out.push(Candidate {
            name: name.to_string(),
            container: container_name,
            kind,
            uri: uri.to_string(),
            path,
            line,
            character,
        });
    }
    out
}

fn as_array(v: &Value) -> Vec<Value> {
    match v {
        Value::Array(a) => a.clone(),
        Value::Null => Vec::new(),
        other => vec![other.clone()],
    }
}

fn range_start(range: &Value) -> Option<(u32, u32)> {
    let start = range.get("start")?;
    let line = start.get("line")?.as_u64()? as u32;
    let character = start.get("character")?.as_u64()? as u32;
    Some((line, character))
}

/// Normalize a definition/references response (single object, array, or null;
/// `Location` or `LocationLink` shapes) into location hits.
fn locations_from(v: &Value, root: &Path) -> Vec<LocationHit> {
    as_array(v)
        .iter()
        .filter_map(|it| location_hit(it, root))
        .collect()
}

fn location_hit(it: &Value, root: &Path) -> Option<LocationHit> {
    let (uri, range) = if let Some(uri) = it.get("uri").and_then(|u| u.as_str()) {
        (uri, it.get("range")?)
    } else if let Some(uri) = it.get("targetUri").and_then(|u| u.as_str()) {
        let range = it
            .get("targetSelectionRange")
            .or_else(|| it.get("targetRange"))?;
        (uri, range)
    } else {
        return None;
    };
    let (line, _) = range_start(range)?;
    Some(make_hit(uri, line, root))
}

/// A call-hierarchy / type-hierarchy item carries `uri` + `selectionRange`.
fn hit_from_item(item: &Value, root: &Path) -> Option<LocationHit> {
    let uri = item.get("uri").and_then(|u| u.as_str())?;
    let range = item.get("selectionRange").or_else(|| item.get("range"))?;
    let (line, _) = range_start(range)?;
    Some(make_hit(uri, line, root))
}

fn make_hit(uri: &str, line0: u32, root: &Path) -> LocationHit {
    let path = display_path(uri, root);
    let snippet = uri_to_path(uri)
        .map(|p| read_line_snippet(&p, line0))
        .unwrap_or_default();
    LocationHit {
        path,
        line: line0 + 1,
        snippet,
    }
}

/// Worktree-relative display path for a `file://` URI, falling back to the
/// absolute path when it is not under `root`.
fn display_path(uri: &str, root: &Path) -> String {
    match uri_to_path(uri) {
        Some(p) => p
            .strip_prefix(root)
            .unwrap_or(&p)
            .to_string_lossy()
            .to_string(),
        None => uri.to_string(),
    }
}

fn read_line_snippet(path: &Path, line0: u32) -> String {
    let Ok(content) = std::fs::read_to_string(path) else {
        return String::new();
    };
    content
        .lines()
        .nth(line0 as usize)
        .map(|l| l.trim().to_string())
        .unwrap_or_default()
}

fn hover_outcome(v: &Value) -> QueryOutcome {
    let text = match v.get("contents") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Object(o)) => o
            .get("value")
            .and_then(|x| x.as_str())
            .map(str::to_string)
            .unwrap_or_default(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|e| match e {
                Value::String(s) => Some(s.clone()),
                Value::Object(o) => o.get("value").and_then(|x| x.as_str()).map(str::to_string),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    };
    if text.trim().is_empty() {
        QueryOutcome::Miss
    } else {
        QueryOutcome::Hover(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsp::client::testkit::{full_capabilities, scripted_client};
    use std::collections::HashMap;

    fn with_init(mut r: HashMap<String, Value>) -> HashMap<String, Value> {
        r.insert("initialize".to_string(), full_capabilities());
        r
    }

    #[test]
    fn split_name_separates_container_and_leaf() {
        assert_eq!(
            split_name("Foo::bar", "::"),
            ("bar".into(), Some("Foo".into()))
        );
        assert_eq!(
            split_name("module.func", "."),
            ("func".into(), Some("module".into()))
        );
        assert_eq!(split_name("plain", "::"), ("plain".into(), None));
        // A trailing separator does not strip the leaf out from under us.
        assert_eq!(split_name("bar", "::"), ("bar".into(), None));
    }

    #[test]
    fn single_symbol_resolves_then_issues_the_op() {
        let mut responses = with_init(HashMap::new());
        responses.insert(
            "workspace/symbol".to_string(),
            json!([{
                "name": "target",
                "kind": 12,
                "location": {
                    "uri": "file:///tmp/lsp-root/src/lib.rs",
                    "range": {"start": {"line": 9, "character": 3}, "end": {"line": 9, "character": 9}}
                }
            }]),
        );
        responses.insert(
            "textDocument/definition".to_string(),
            json!({
                "uri": "file:///tmp/lsp-root/src/lib.rs",
                "range": {"start": {"line": 9, "character": 3}, "end": {"line": 9, "character": 9}}
            }),
        );
        let (client, recorded) = scripted_client(responses, true);
        let (outcome, still_indexing) = run_named_query(
            &client,
            Path::new("/tmp/lsp-root"),
            LspOp::Definition,
            "target",
            "::",
        )
        .unwrap();
        assert!(!still_indexing);
        match outcome {
            QueryOutcome::Locations(hits) => {
                assert_eq!(hits.len(), 1);
                assert_eq!(hits[0].path, "src/lib.rs");
                assert_eq!(hits[0].line, 10);
            }
            other => panic!("expected locations, got {other:?}"),
        }
        // The op request was actually issued after resolution.
        assert!(recorded
            .lock()
            .unwrap()
            .contains(&"textDocument/definition".to_string()));
    }

    #[test]
    fn ambiguous_name_returns_candidates_and_issues_no_op() {
        let mut responses = with_init(HashMap::new());
        responses.insert(
            "workspace/symbol".to_string(),
            json!([
                {"name": "dup", "kind": 12, "containerName": "a",
                 "location": {"uri": "file:///tmp/lsp-root/a.rs",
                   "range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 3}}}},
                {"name": "dup", "kind": 12, "containerName": "b",
                 "location": {"uri": "file:///tmp/lsp-root/b.rs",
                   "range": {"start": {"line": 4, "character": 0}, "end": {"line": 4, "character": 3}}}}
            ]),
        );
        let (client, recorded) = scripted_client(responses, true);
        let (outcome, _) = run_named_query(
            &client,
            Path::new("/tmp/lsp-root"),
            LspOp::Definition,
            "dup",
            "::",
        )
        .unwrap();
        match outcome {
            QueryOutcome::Candidates(cands) => assert_eq!(cands.len(), 2),
            other => panic!("expected candidates, got {other:?}"),
        }
        // Crucially, no position op was issued for the ambiguous name.
        assert!(!recorded
            .lock()
            .unwrap()
            .contains(&"textDocument/definition".to_string()));
    }

    #[test]
    fn container_qualified_name_filters_candidates() {
        let mut responses = with_init(HashMap::new());
        responses.insert(
            "workspace/symbol".to_string(),
            json!([
                {"name": "dup", "kind": 12, "containerName": "a",
                 "location": {"uri": "file:///tmp/lsp-root/a.rs",
                   "range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 3}}}},
                {"name": "dup", "kind": 12, "containerName": "b",
                 "location": {"uri": "file:///tmp/lsp-root/b.rs",
                   "range": {"start": {"line": 4, "character": 0}, "end": {"line": 4, "character": 3}}}}
            ]),
        );
        responses.insert(
            "textDocument/definition".to_string(),
            json!({"uri": "file:///tmp/lsp-root/b.rs",
                   "range": {"start": {"line": 4, "character": 0}, "end": {"line": 4, "character": 3}}}),
        );
        let (client, _) = scripted_client(responses, true);
        // `b::dup` disambiguates to the single b-container symbol.
        let (outcome, _) = run_named_query(
            &client,
            Path::new("/tmp/lsp-root"),
            LspOp::Definition,
            "b::dup",
            "::",
        )
        .unwrap();
        match outcome {
            QueryOutcome::Locations(hits) => {
                assert_eq!(hits.len(), 1);
                assert_eq!(hits[0].path, "b.rs");
            }
            other => panic!("expected single location, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_op_short_circuits() {
        // Server advertises only definition.
        let mut responses = HashMap::new();
        responses.insert(
            "initialize".to_string(),
            json!({"capabilities": {"definitionProvider": true}}),
        );
        let (client, recorded) = scripted_client(responses, true);
        let (outcome, _) = run_named_query(
            &client,
            Path::new("/tmp/lsp-root"),
            LspOp::Subtypes,
            "X",
            "::",
        )
        .unwrap();
        assert_eq!(outcome, QueryOutcome::Unsupported);
        // No workspace/symbol lookup happened for an unsupported op.
        assert!(!recorded
            .lock()
            .unwrap()
            .contains(&"workspace/symbol".to_string()));
    }

    #[test]
    fn hover_extracts_markup_value() {
        let mut responses = with_init(HashMap::new());
        responses.insert(
            "workspace/symbol".to_string(),
            json!([{"name": "t", "kind": 12,
                "location": {"uri": "file:///tmp/lsp-root/x.rs",
                  "range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 1}}}}]),
        );
        responses.insert(
            "textDocument/hover".to_string(),
            json!({"contents": {"kind": "markdown", "value": "```rust\nfn t()\n```"}}),
        );
        let (client, _) = scripted_client(responses, true);
        let (outcome, _) =
            run_named_query(&client, Path::new("/tmp/lsp-root"), LspOp::Hover, "t", "::").unwrap();
        match outcome {
            QueryOutcome::Hover(md) => assert!(md.contains("fn t()")),
            other => panic!("expected hover, got {other:?}"),
        }
    }

    #[test]
    fn search_symbols_returns_candidates_without_op() {
        let mut responses = with_init(HashMap::new());
        responses.insert(
            "workspace/symbol".to_string(),
            json!([{
                "name": "build_widget", "kind": 12,
                "location": {"uri": "file:///tmp/lsp-root/src/lib.rs",
                  "range": {"start": {"line": 14, "character": 7}, "end": {"line": 14, "character": 19}}}
            }]),
        );
        let (client, recorded) = scripted_client(responses, true);
        let outcome = search_symbols(&client, Path::new("/tmp/lsp-root"), "build_widget").unwrap();
        match outcome {
            QueryOutcome::Candidates(candidates) => {
                assert_eq!(candidates.len(), 1);
                assert_eq!(candidates[0].name, "build_widget");
                assert_eq!(candidates[0].path, "src/lib.rs");
            }
            other => panic!("expected candidates, got {other:?}"),
        }
        // Discovery issues no position op.
        assert!(!recorded
            .lock()
            .unwrap()
            .contains(&"textDocument/definition".to_string()));
    }

    #[test]
    fn compute_rename_returns_workspace_edit() {
        let mut responses = HashMap::new();
        responses.insert(
            "initialize".to_string(),
            json!({"capabilities": {"renameProvider": true}}),
        );
        responses.insert(
            "textDocument/rename".to_string(),
            json!({"changes": {"file:///tmp/lsp-root/a.rs": [
                {"range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 3}},
                 "newText": "new"}
            ]}}),
        );
        let (client, recorded) = scripted_client(responses, true);
        let edit = compute_rename(&client, "file:///tmp/lsp-root/a.rs", (0, 0), "new").unwrap();
        assert!(edit.get("changes").is_some());
        assert!(recorded
            .lock()
            .unwrap()
            .contains(&"textDocument/rename".to_string()));
    }

    #[test]
    fn compute_rename_unsupported_without_capability() {
        let mut responses = HashMap::new();
        responses.insert(
            "initialize".to_string(),
            json!({"capabilities": {"definitionProvider": true}}),
        );
        let (client, recorded) = scripted_client(responses, true);
        let err = compute_rename(&client, "file:///tmp/lsp-root/a.rs", (0, 0), "new").unwrap_err();
        assert!(matches!(
            err,
            LspError::Unsupported {
                op: LspOp::Rename,
                ..
            }
        ));
        // No rename request issued for an unsupported capability.
        assert!(!recorded
            .lock()
            .unwrap()
            .contains(&"textDocument/rename".to_string()));
    }

    #[test]
    fn resolve_rename_position_single_and_ambiguous() {
        let mut single = with_init(HashMap::new());
        single.insert(
            "workspace/symbol".to_string(),
            json!([{"name": "target", "kind": 12,
                "location": {"uri": "file:///tmp/lsp-root/a.rs",
                  "range": {"start": {"line": 7, "character": 4}, "end": {"line": 7, "character": 10}}}}]),
        );
        let (client, _) = scripted_client(single, true);
        match resolve_rename_position(&client, Path::new("/tmp/lsp-root"), "target", "::").unwrap() {
            RenameTarget::Resolved { uri, position } => {
                assert_eq!(uri, "file:///tmp/lsp-root/a.rs");
                assert_eq!(position, (7, 4));
            }
            other => panic!("expected resolved, got {other:?}"),
        }

        let mut ambiguous = with_init(HashMap::new());
        ambiguous.insert(
            "workspace/symbol".to_string(),
            json!([
                {"name": "dup", "kind": 12, "containerName": "a",
                 "location": {"uri": "file:///tmp/lsp-root/a.rs",
                   "range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 3}}}},
                {"name": "dup", "kind": 12, "containerName": "b",
                 "location": {"uri": "file:///tmp/lsp-root/b.rs",
                   "range": {"start": {"line": 4, "character": 0}, "end": {"line": 4, "character": 3}}}}
            ]),
        );
        let (client, _) = scripted_client(ambiguous, true);
        match resolve_rename_position(&client, Path::new("/tmp/lsp-root"), "dup", "::").unwrap() {
            RenameTarget::Ambiguous(candidates) => assert_eq!(candidates.len(), 2),
            other => panic!("expected ambiguous, got {other:?}"),
        }
    }

    #[test]
    fn resolve_in_file_hits_hierarchical_and_misses_cleanly() {
        let mut responses = with_init(HashMap::new());
        responses.insert(
            "textDocument/documentSymbol".to_string(),
            json!([{
                "name": "Widget", "kind": 23,
                "range": {"start": {"line": 0, "character": 0}, "end": {"line": 5, "character": 0}},
                "selectionRange": {"start": {"line": 0, "character": 11}, "end": {"line": 0, "character": 17}},
                "children": [{
                    "name": "area", "kind": 6,
                    "range": {"start": {"line": 2, "character": 4}, "end": {"line": 4, "character": 5}},
                    "selectionRange": {"start": {"line": 2, "character": 11}, "end": {"line": 2, "character": 15}}
                }]
            }]),
        );
        let (client, _) = scripted_client(responses, true);
        let hit = resolve_in_file(
            &client,
            Path::new("/tmp/lsp-root"),
            "file:///tmp/lsp-root/src/lib.rs",
            "area",
            "::",
        )
        .unwrap();
        assert_eq!(
            hit,
            Some(("file:///tmp/lsp-root/src/lib.rs".to_string(), (2, 11)))
        );

        let miss = resolve_in_file(
            &client,
            Path::new("/tmp/lsp-root"),
            "file:///tmp/lsp-root/src/lib.rs",
            "nonexistent",
            "::",
        )
        .unwrap();
        assert_eq!(miss, None);
    }
}
