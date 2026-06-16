//! Thin `Orchestrator` methods over the LSP engine (`crate::lsp`).
//!
//! Mirrors the `build_services` Orchestrator block: read settings, route the
//! file to a language server, resolve the indexing root, spawn-or-reuse the
//! pooled instance, run the query, and render a read-style block. The heavy
//! lifting lives in `crate::lsp`; these methods just bind it to settings, the
//! shared process spawner, and the per-host cache directory.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::language_servers::LanguageServerConfig;
use crate::config::settings;
use crate::lsp::client::LspClient;
use crate::lsp::manager::{LspInstance, LspInstanceStatus};
use crate::lsp::queries::{self, Candidate, LocationHit, QueryOutcome};
use crate::lsp::render::{self, Rendered};
use crate::lsp::{routing, InstanceKey, LspOp, Unavailable};

use super::Orchestrator;

/// How many directory levels below the worktree root the fan-out marker scan
/// descends to discover a language's workspace root (e.g. `src-tauri/Cargo.toml`
/// in a JS-rooted repo).
const MARKER_SCAN_DEPTH: usize = 4;

/// How deep to scan for a representative source file to open before a
/// `workspace/symbol` query. Breadth-first, so shallow files are found first
/// and this only bounds the worst case on a tree with no nearby source.
const REPR_SCAN_DEPTH: usize = 6;

/// A language server selected for a query, carrying both the worktree-relative
/// indexing root and the *effective* root the pooled instance is actually keyed
/// and confined on. When a worktree subroot is byte-identical to the project's
/// main checkout, the effective root is the main-checkout subroot, so equivalent
/// worktrees collapse onto one warm instance (`rerouted == true`); otherwise the
/// effective root is the worktree subroot and behavior is exactly per-worktree.
struct RoutedServer {
    language: String,
    cfg: LanguageServerConfig,
    /// The agent worktree this query runs against (template base when not
    /// rerouted, and the base for relativizing rendered paths).
    worktree: PathBuf,
    /// The worktree-absolute indexing root resolved from markers.
    worktree_root: PathBuf,
    /// The root the instance keys/confines on: the main-checkout subroot when
    /// `rerouted`, else `worktree_root`.
    effective_root: PathBuf,
    rerouted: bool,
}

impl RoutedServer {
    /// Map a worktree file to the checkout the server is confined to. When
    /// rerouted the server lives at the main checkout, so a file under the
    /// worktree subroot is remapped to the byte-identical base copy; positions
    /// map exactly. Unchanged when not rerouted.
    fn translate_file(&self, file: &Path) -> PathBuf {
        if !self.rerouted {
            return file.to_path_buf();
        }
        match file.strip_prefix(&self.worktree_root) {
            Ok(rel) => self.effective_root.join(rel),
            Err(_) => file.to_path_buf(),
        }
    }

    /// The worktree used for service-template expansion at spawn time. When
    /// rerouted the server is confined to the base checkout, so templates must
    /// resolve inside it.
    fn template_worktree(&self) -> &Path {
        if self.rerouted {
            &self.effective_root
        } else {
            &self.worktree
        }
    }

    /// Reverse of [`RoutedServer::translate_file`]: map a path the server
    /// produced back onto the worktree, or `None` when it falls outside the
    /// rename's allowed root. Not rerouted: accept paths under the worktree
    /// indexing root unchanged. Rerouted: the server indexed the byte-identical
    /// main checkout, so strip the effective (base) root and rejoin the worktree
    /// root; a path outside the base subroot returns `None` so out-of-scope
    /// edits are rejected before anything is written.
    fn translate_back(&self, file: &Path) -> Option<PathBuf> {
        if !self.rerouted {
            return if file.starts_with(&self.worktree_root) {
                Some(file.to_path_buf())
            } else {
                None
            };
        }
        file.strip_prefix(&self.effective_root)
            .ok()
            .map(|rel| self.worktree_root.join(rel))
    }
}

/// How the symbol to rename is located: by a bare name (resolved through
/// `workspace/symbol`, refusing to guess on ambiguity) or by an explicit
/// document position (which disambiguates overloads/shadowing).
pub enum RenameSpec {
    Name(String),
    At(PathBuf, (u32, u32)),
}

/// The computed set of file edits a rename will apply, ready for the change
/// handler to turn into worktree writes and one commit.
pub struct RenamePlan {
    pub file_edits: Vec<crate::lsp::edit::FileEdit>,
}

/// Render an ambiguous-name rejection: the candidate sites plus how to
/// disambiguate. No rename is issued for an ambiguous bare name.
fn ambiguous_rename_message(name: &str, candidates: &[Candidate]) -> String {
    let mut msg = format!(
        "`{name}` is ambiguous ({} symbols match); rename by position with \
         symbol_at:\"file:PATH:LINE\" or a container-qualified name. Candidates:",
        candidates.len()
    );
    for candidate in candidates {
        msg.push_str(&format!("\n  - {}:{}", candidate.path, candidate.display_line()));
        if let Some(container) = &candidate.container {
            msg.push_str(&format!(" (in {container})"));
        }
    }
    msg
}

impl Orchestrator {
    /// Answer an LSP query for `name` against `file` in `worktree`, returning a
    /// read-style block (`=== lsp:<op>/<name> [<suffix>] ===` + body). Failures
    /// (no configured server, unavailable engine, transport error) render as a
    /// descriptive block rather than propagating, so the Phase-3 read surface
    /// can compose them uniformly.
    pub fn lsp_query(&self, worktree: &Path, file: &Path, op: LspOp, name: &str) -> String {
        let descriptor = format!("lsp:{}/{name}", op.as_str());
        match self.lsp_query_inner(worktree, file, op, name) {
            Ok(block) => block,
            Err(message) => format!("=== {descriptor} ===\n{message}"),
        }
    }

    fn lsp_query_inner(
        &self,
        worktree: &Path,
        file: &Path,
        op: LspOp,
        name: &str,
    ) -> Result<String, String> {
        let servers = settings::load_language_servers(&self.config_dir);
        let ext = file.extension().and_then(|e| e.to_str()).unwrap_or("");
        let (language, cfg) = routing::route_extension(&servers, ext)
            .ok_or_else(|| format!("no language server configured for .{ext} files"))?;
        let language = language.clone();
        let cfg = cfg.clone();

        let root = routing::resolve_root(file, worktree, &cfg.root_markers);
        let routed = self.routed_server(worktree, language.clone(), cfg.clone(), root);

        let instance = self.spawn_instance(&routed).map_err(|u| u.reason)?;

        instance
            .client
            .ensure_open(&routed.translate_file(file))
            .map_err(|e| e.to_string())?;

        let separator = routed.cfg.container_separator.clone();
        let (outcome, still_indexing) = queries::run_named_query(
            &instance.client,
            &routed.effective_root,
            op,
            name,
            &separator,
        )
        .map_err(|e| e.to_string())?;

        let rendered = render::render(&outcome, still_indexing);
        let descriptor = format!("lsp:{}/{name}", op.as_str());
        Ok(render::to_block(&descriptor, &rendered))
    }

    /// Compute a semantic rename plan for the symbol identified by `spec` in
    /// `file`, renaming it to `new_name`. Routes by the file's extension to the
    /// single configured language server (rename is single-symbol/single-language
    /// — no cross-language fan-out), resolves the symbol's position, asks the
    /// server for the `WorkspaceEdit`, and translates every edit back onto the
    /// worktree. A write path: it returns a plan to apply, not a rendered read
    /// block. Honest `Err(String)` for no configured server, an unavailable
    /// server, an ambiguous or missing name, an unsupported or refused rename, or
    /// an edit that would escape the worktree.
    pub fn lsp_rename(
        &self,
        worktree: &Path,
        file: &Path,
        spec: RenameSpec,
        new_name: &str,
    ) -> Result<RenamePlan, String> {
        let servers = settings::load_language_servers(&self.config_dir);
        let ext = file.extension().and_then(|e| e.to_str()).unwrap_or("");
        let (language, cfg) = routing::route_extension(&servers, ext)
            .ok_or_else(|| format!("no language server configured for .{ext} files"))?;
        let language = language.clone();
        let cfg = cfg.clone();

        let root = routing::resolve_root(file, worktree, &cfg.root_markers);
        let routed = self.routed_server(worktree, language, cfg, root);
        let instance = self.ready_instance(&routed)?;

        let (uri, position) = match spec {
            RenameSpec::At(path, position) => {
                let opened = instance
                    .client
                    .ensure_open(&routed.translate_file(&path))
                    .map_err(|e| e.to_string())?;
                (opened, position)
            }
            RenameSpec::Name(name) => {
                // Open the routing file so the server has a document anchored in
                // the root; name resolution itself goes via workspace/symbol.
                instance
                    .client
                    .ensure_open(&routed.translate_file(file))
                    .map_err(|e| e.to_string())?;
                match queries::resolve_rename_position(
                    &instance.client,
                    &routed.effective_root,
                    &name,
                    &routed.cfg.container_separator,
                )
                .map_err(|e| e.to_string())?
                {
                    queries::RenameTarget::Resolved { uri, position } => (uri, position),
                    queries::RenameTarget::Miss => {
                        return Err(format!("no symbol named `{name}` found to rename"))
                    }
                    queries::RenameTarget::Ambiguous(candidates) => {
                        return Err(ambiguous_rename_message(&name, &candidates))
                    }
                }
            }
        };

        let edit = queries::compute_rename(&instance.client, &uri, position, new_name)
            .map_err(|e| e.to_string())?;
        let translate = |path: &Path| routed.translate_back(path);
        let file_edits = crate::lsp::edit::plan_workspace_edit(&edit, &translate)
            .map_err(|e| e.to_string())?;
        if file_edits.is_empty() {
            return Err("the language server returned no edits for this rename".to_string());
        }
        Ok(RenamePlan { file_edits })
    }

    /// Build a [`RoutedServer`], deciding whether the worktree subroot collapses
    /// onto the project's main-checkout instance.
    fn routed_server(
        &self,
        worktree: &Path,
        language: String,
        cfg: LanguageServerConfig,
        worktree_root: PathBuf,
    ) -> RoutedServer {
        let (effective_root, rerouted) = self.resolve_effective_root(worktree, &worktree_root);
        RoutedServer {
            language,
            cfg,
            worktree: worktree.to_path_buf(),
            worktree_root,
            effective_root,
            rerouted,
        }
    }

    /// Resolve the project's main checkout from a linked worktree via the shared
    /// git common dir. `None` when `worktree` *is* the main checkout (its common
    /// dir's parent is itself) or resolution fails — in either case there is no
    /// other checkout to reroute to.
    fn base_checkout(&self, worktree: &Path) -> Option<PathBuf> {
        let common = self
            .services
            .git
            .rev_parse(
                worktree,
                vec![
                    "--path-format=absolute".to_string(),
                    "--git-common-dir".to_string(),
                ],
            )
            .ok()?;
        let common = common.trim();
        if common.is_empty() {
            return None;
        }
        let repo_path = Path::new(common).parent()?.to_path_buf();
        // Canonicalize both sides before comparing and returning: git reports the
        // real path (e.g. macOS resolves /var -> /private/var), so a symlinked
        // worktree path must not hide that the worktree *is* the main checkout,
        // and the rerouted root must be stable regardless of how the worktree
        // path was expressed.
        let repo_path = std::fs::canonicalize(&repo_path).unwrap_or(repo_path);
        let worktree_canon =
            std::fs::canonicalize(worktree).unwrap_or_else(|_| worktree.to_path_buf());
        if repo_path == worktree_canon {
            return None;
        }
        Some(repo_path)
    }

    /// Decide the effective indexing root for a worktree subroot: the equivalent
    /// main-checkout subroot when content matches (collapsing onto the shared
    /// base instance), else the worktree subroot unchanged. Returns
    /// `(effective_root, rerouted)`. The optimization only ever turns *off*: any
    /// divergence, dirt, missing base subroot, or git error falls back to the
    /// per-worktree root, so it is never load-bearing for correctness.
    fn resolve_effective_root(&self, worktree: &Path, worktree_root: &Path) -> (PathBuf, bool) {
        let fallback = || (worktree_root.to_path_buf(), false);
        let Some(repo_path) = self.base_checkout(worktree) else {
            return fallback();
        };
        let Ok(relpath) = worktree_root.strip_prefix(worktree) else {
            return fallback();
        };
        let base_root = repo_path.join(relpath);
        if !base_root.exists() {
            return fallback();
        }
        let relpath_str = relpath.to_string_lossy().replace('\\', "/");
        if routing::subroot_equivalent(
            self.services.git.as_ref(),
            worktree,
            &relpath_str,
            &repo_path,
            &relpath_str,
        ) {
            (base_root, true)
        } else {
            fallback()
        }
    }

    /// Spawn or reuse the pooled language-server instance for `(language, root)`,
    /// confined exactly like a worktree run. The single spawn path shared by the
    /// file-anchored `lsp_query` and every Phase-3 read-surface method.
    fn spawn_instance(&self, routed: &RoutedServer) -> Result<Arc<LspInstance>, Unavailable> {
        let key = InstanceKey::new(routed.language.clone(), routed.effective_root.clone());
        let templates = settings::build_service_templates(
            &self.config_dir,
            Some(routed.template_worktree().to_path_buf()),
        );
        let cache_dir = self.lsp_cache_dir(&key);
        self.lsp_manager.get_or_spawn(
            self.services.process.as_ref(),
            key,
            &routed.cfg,
            &templates,
            &cache_dir,
            self.sandbox_deny_read(),
        )
    }

    /// Spawn (or reuse) the instance and confirm it answered the `initialize`
    /// handshake. Returns an honest, actionable message when the server could not
    /// be reached — missing binary, crash on startup, or a failed handshake (with
    /// a stderr tail) — so the read surface never silently degrades a dead server
    /// into "no results" or leaks a raw transport error.
    fn ready_instance(&self, routed: &RoutedServer) -> Result<Arc<LspInstance>, String> {
        match self.spawn_instance(routed) {
            Ok(instance) if instance.client.handshake_ok() => Ok(instance),
            Ok(instance) => Err(server_failure_message(&routed.language, &instance)),
            Err(unavailable) => Err(format!(
                "`{}` language server unavailable: {}",
                routed.language, unavailable.reason
            )),
        }
    }

    /// Select the language servers to drive for a name/search query.
    ///
    /// A `hint` path with a routable extension pins exactly that language (root
    /// resolved from the hint); a hint that carries an extension but matches no
    /// configured server yields nothing (honest empty). Otherwise this fans out to
    /// every enabled language whose root marker is actually present *anywhere*
    /// within the worktree (bounded depth), rooting each server at the directory
    /// that holds the marker. A worktree whose Rust workspace lives under
    /// `src-tauri/` therefore still spawns rust-analyzer there; a language with no
    /// marker present is never spawned (no index paid to find nothing).
    fn lsp_route_servers(&self, worktree: &Path, hint: Option<&Path>) -> Vec<RoutedServer> {
        let servers = settings::load_language_servers(&self.config_dir);
        if let Some(path) = hint {
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            if !ext.is_empty() {
                return match routing::route_extension(&servers, ext) {
                    Some((language, cfg)) => {
                        let root = routing::resolve_root(path, worktree, &cfg.root_markers);
                        vec![self.routed_server(worktree, language.clone(), cfg.clone(), root)]
                    }
                    None => Vec::new(),
                };
            }
            // A directory hint (no extension) narrows diagnostics, not the index
            // root; fall through to marker-gated fan-out across the worktree.
        }
        let mut enabled: Vec<(&String, &LanguageServerConfig)> =
            servers.iter().filter(|(_, cfg)| cfg.enabled).collect();
        enabled.sort_by(|a, b| a.0.cmp(b.0));
        let mut routed = Vec::new();
        for (language, cfg) in enabled {
            for root in routing::find_marker_roots(worktree, &cfg.root_markers, MARKER_SCAN_DEPTH) {
                routed.push(self.routed_server(worktree, language.clone(), cfg.clone(), root));
            }
        }
        routed
    }

    fn lsp_text_search_pointer(scope: &str) -> Rendered {
        Rendered {
            body: format!("no language server for {scope} — use text search (read with ?grep=)"),
            suffix: None,
        }
    }

    fn ext_scope(file: &Path) -> String {
        match file.extension().and_then(|e| e.to_str()) {
            Some(ext) if !ext.is_empty() => format!(".{ext} files"),
            _ => "this file".to_string(),
        }
    }

    /// Fuzzy workspace-symbol discovery across the routed servers. Empty routable
    /// set → honest text-search pointer; empty result set → a clean miss.
    pub fn lsp_search(&self, worktree: &Path, query: &str, in_hint: Option<&Path>) -> Rendered {
        let servers = self.lsp_route_servers(worktree, in_hint);
        if servers.is_empty() {
            return Self::lsp_text_search_pointer("this worktree");
        }
        let mut candidates: Vec<Candidate> = Vec::new();
        let mut failures: Vec<String> = Vec::new();
        for routed in &servers {
            match self.ready_instance(routed) {
                Ok(instance) => {
                    // workspace/symbol needs an open document to establish a
                    // project-based server's project (tsserver throws "No Project"
                    // cold). Prewarm usually did this already; this is the backstop
                    // for an un-prewarmed or raced instance — guarded no-op otherwise.
                    self.ensure_project_open(&instance, routed, worktree, in_hint);
                    match queries::search_symbols(&instance.client, &routed.effective_root, query) {
                        Ok(QueryOutcome::Candidates(found)) => candidates.extend(found),
                        Ok(_) => {}
                        Err(error) => failures.push(format!("`{}`: {error}", routed.language)),
                    }
                }
                Err(message) => failures.push(message),
            }
        }
        if !candidates.is_empty() {
            return render::render(&QueryOutcome::Candidates(candidates), false);
        }
        render_miss_or_failures(failures)
    }

    /// Name/position-based op across the routed servers. `at` is the position
    /// escape hatch (route by the file's extension, no name resolution); else the
    /// name is resolved per server and `Locations`/`Candidates` merged so
    /// cross-language disambiguation falls out naturally. `op: None` is an
    /// overview (definition + hover).
    pub fn lsp_named(
        &self,
        worktree: &Path,
        op: Option<LspOp>,
        name: &str,
        in_hint: Option<&Path>,
        at: Option<(PathBuf, (u32, u32))>,
    ) -> Rendered {
        if let Some((file, position)) = at {
            let servers = self.lsp_route_servers(worktree, Some(&file));
            let Some(routed) = servers.into_iter().next() else {
                return Self::lsp_text_search_pointer(&Self::ext_scope(&file));
            };
            let instance = match self.ready_instance(&routed) {
                Ok(instance) => instance,
                Err(message) => {
                    return Rendered {
                        body: message,
                        suffix: None,
                    }
                }
            };
            let file_uri = match instance.client.ensure_open(&routed.translate_file(&file)) {
                Ok(uri) => uri,
                Err(error) => {
                    return Rendered {
                        body: error.to_string(),
                        suffix: None,
                    }
                }
            };
            return self.op_or_overview_at(
                &instance.client,
                &routed.effective_root,
                &file_uri,
                position,
                op,
            );
        }

        let servers = self.lsp_route_servers(worktree, in_hint);
        if servers.is_empty() {
            return Self::lsp_text_search_pointer("this worktree");
        }
        let Some(op) = op else {
            return self.named_overview(name, &servers);
        };

        let mut locations: Vec<LocationHit> = Vec::new();
        let mut candidates: Vec<Candidate> = Vec::new();
        let mut hover: Option<String> = None;
        let mut still_indexing = false;
        let mut unsupported = false;
        let mut failures: Vec<String> = Vec::new();
        for routed in &servers {
            let instance = match self.ready_instance(routed) {
                Ok(instance) => instance,
                Err(message) => {
                    failures.push(message);
                    continue;
                }
            };
            let separator = routed.cfg.container_separator.clone();
            match queries::run_named_query(
                &instance.client,
                &routed.effective_root,
                op,
                name,
                &separator,
            ) {
                Ok((outcome, si)) => {
                    still_indexing |= si;
                    match outcome {
                        QueryOutcome::Locations(found) => locations.extend(found),
                        QueryOutcome::Candidates(found) => candidates.extend(found),
                        QueryOutcome::Hover(text) => {
                            hover.get_or_insert(text);
                        }
                        QueryOutcome::Miss => {}
                        QueryOutcome::Unsupported => unsupported = true,
                    }
                }
                Err(error) => failures.push(format!("`{}`: {error}", routed.language)),
            }
        }
        let merged = if !locations.is_empty() {
            QueryOutcome::Locations(locations)
        } else if !candidates.is_empty() {
            QueryOutcome::Candidates(candidates)
        } else if let Some(text) = hover {
            QueryOutcome::Hover(text)
        } else if unsupported {
            QueryOutcome::Unsupported
        } else if !failures.is_empty() {
            // Nothing answered and a server could not be reached: surface why,
            // not an indistinguishable "no results".
            return Rendered {
                body: failures.join("\n"),
                suffix: None,
            };
        } else {
            QueryOutcome::Miss
        };
        render::render(&merged, still_indexing)
    }

    /// Name-based overview: definition locations + hover, merged across servers.
    /// Ambiguity wins — a multi-symbol name surfaces the candidate list.
    fn named_overview(&self, name: &str, servers: &[RoutedServer]) -> Rendered {
        let mut locations: Vec<LocationHit> = Vec::new();
        let mut candidates: Vec<Candidate> = Vec::new();
        let mut hover: Option<String> = None;
        let mut still_indexing = false;
        let mut failures: Vec<String> = Vec::new();
        for routed in servers {
            let instance = match self.ready_instance(routed) {
                Ok(instance) => instance,
                Err(message) => {
                    failures.push(message);
                    continue;
                }
            };
            let separator = routed.cfg.container_separator.clone();
            let root = &routed.effective_root;
            if let Ok((outcome, si)) = queries::run_named_query(
                &instance.client,
                root,
                LspOp::Definition,
                name,
                &separator,
            ) {
                still_indexing |= si;
                match outcome {
                    QueryOutcome::Locations(found) => locations.extend(found),
                    QueryOutcome::Candidates(found) => candidates.extend(found),
                    _ => {}
                }
            }
            if let Ok((QueryOutcome::Hover(text), _)) =
                queries::run_named_query(&instance.client, root, LspOp::Hover, name, &separator)
            {
                hover.get_or_insert(text);
            }
        }
        if locations.is_empty() && !candidates.is_empty() {
            return render::render(&QueryOutcome::Candidates(candidates), still_indexing);
        }
        if locations.is_empty() && hover.is_none() && !failures.is_empty() {
            return Rendered {
                body: failures.join("\n"),
                suffix: None,
            };
        }
        compose_overview(&locations, hover.as_deref(), still_indexing)
    }

    /// Run `op` at a concrete document position, or compose an overview
    /// (definition + hover) when `op` is `None`.
    fn op_or_overview_at(
        &self,
        client: &LspClient,
        root: &Path,
        file_uri: &str,
        position: (u32, u32),
        op: Option<LspOp>,
    ) -> Rendered {
        match op {
            Some(op) => match queries::execute_op_at(client, root, op, file_uri, position) {
                Ok(outcome) => render::render(&outcome, false),
                Err(error) => Rendered {
                    body: error.to_string(),
                    suffix: None,
                },
            },
            None => {
                let locations = match queries::execute_op_at(
                    client,
                    root,
                    LspOp::Definition,
                    file_uri,
                    position,
                ) {
                    Ok(QueryOutcome::Locations(hits)) => hits,
                    _ => Vec::new(),
                };
                let hover =
                    match queries::execute_op_at(client, root, LspOp::Hover, file_uri, position) {
                        Ok(QueryOutcome::Hover(text)) => Some(text),
                        _ => None,
                    };
                compose_overview(&locations, hover.as_deref(), false)
            }
        }
    }

    /// Aggregate passively-collected diagnostics from already-pooled instances
    /// serving this worktree's routed subroots (optionally filtered to
    /// `in_hint`). v1 reads only warm instances — a cold query with no live
    /// server reports none collected rather than spawning and waiting.
    ///
    /// Each routed subroot resolves to its *effective* root, so a node whose
    /// subroot collapsed onto the main-checkout instance still finds its
    /// diagnostics there; hits are mapped back from the (possibly base) root to
    /// worktree-relative paths.
    pub fn lsp_diagnostics(&self, worktree: &Path, in_hint: Option<&Path>) -> Rendered {
        let filter = in_hint.map(|path| {
            if path.is_absolute() {
                path.to_path_buf()
            } else {
                worktree.join(path)
            }
        });
        let routed = self.lsp_route_servers(worktree, in_hint);
        let instances = self.lsp_manager.instances();
        let mut hits: Vec<LocationHit> = Vec::new();
        for server in &routed {
            let key = InstanceKey::new(server.language.clone(), server.effective_root.clone());
            let Some(instance) = instances.iter().find(|inst| inst.key == key) else {
                continue;
            };
            for frame in instance.client.diagnostics() {
                collect_diagnostic_hits(
                    &frame,
                    worktree,
                    &server.effective_root,
                    &server.worktree_root,
                    filter.as_deref(),
                    &mut hits,
                );
            }
        }
        if hits.is_empty() {
            return Rendered {
                body: "no diagnostics collected (open a file to populate diagnostics)".to_string(),
                suffix: Some("0 matches".to_string()),
            };
        }
        render::render(&QueryOutcome::Locations(hits), false)
    }

    /// File-scoped op for the `file:...?lsp=` projection: route by the file's own
    /// extension, resolve the symbol locally via `documentSymbol` (or use an
    /// explicit `at` position), then run the op. No server for the extension →
    /// honest text-search pointer.
    pub fn lsp_file_op(
        &self,
        worktree: &Path,
        file: &Path,
        op: Option<LspOp>,
        symbol: Option<&str>,
        at: Option<(u32, u32)>,
    ) -> Rendered {
        let servers = self.lsp_route_servers(worktree, Some(file));
        let Some(routed) = servers.into_iter().next() else {
            return Self::lsp_text_search_pointer(&Self::ext_scope(file));
        };
        let instance = match self.ready_instance(&routed) {
            Ok(instance) => instance,
            Err(message) => {
                return Rendered {
                    body: message,
                    suffix: None,
                }
            }
        };
        let file_uri = match instance.client.ensure_open(&routed.translate_file(file)) {
            Ok(uri) => uri,
            Err(error) => {
                return Rendered {
                    body: error.to_string(),
                    suffix: None,
                }
            }
        };
        let position = if let Some(position) = at {
            position
        } else {
            let Some(symbol) = symbol else {
                return Rendered {
                    body: "provide a symbol (?symbol=) or a position (?at=) to query".to_string(),
                    suffix: None,
                };
            };
            match queries::resolve_in_file(
                &instance.client,
                &routed.effective_root,
                &file_uri,
                symbol,
                &routed.cfg.container_separator,
            ) {
                Ok(Some((_, position))) => position,
                Ok(None) => return render::render(&QueryOutcome::Miss, false),
                Err(error) => {
                    return Rendered {
                        body: error.to_string(),
                        suffix: None,
                    }
                }
            }
        };
        self.op_or_overview_at(
            &instance.client,
            &routed.effective_root,
            &file_uri,
            position,
            op,
        )
    }

    /// Eagerly spawn the language servers a worktree routes to so indexing is
    /// already under way (often finished) before an agent issues its first lsp
    /// query. Best-effort: a missing binary or absent OS sandbox is skipped, and
    /// the heavy workspace indexing proceeds inside the server process, so this
    /// returns once the (fast) `initialize` handshakes complete. Idempotent via
    /// the pool — once warm, repeat calls are cheap no-ops. A worktree with no
    /// recognized language markers spawns nothing.
    pub fn lsp_prewarm(&self, worktree: &Path) {
        for routed in self.lsp_route_servers(worktree, None) {
            if let Ok(instance) = self.spawn_instance(&routed) {
                self.ensure_project_open(&instance, &routed, worktree, None);
            }
        }
    }

    /// Open a representative document on `instance` so a project-based server
    /// (tsserver) has an established project before `workspace/symbol`. A guarded
    /// no-op once the instance has any open document — from prewarm, a prior
    /// file-anchored read, or a prior search — so it neither rescans nor disturbs
    /// an eager server (rust-analyzer). Prefer the caller's `in` hint when it is a
    /// file the routed server handles; otherwise the shallowest matching source
    /// file under the effective root. Best-effort: a missing file is a silent skip.
    fn ensure_project_open(
        &self,
        instance: &LspInstance,
        routed: &RoutedServer,
        worktree: &Path,
        in_hint: Option<&Path>,
    ) {
        if instance.client.has_open_documents() {
            return;
        }
        let file = self.hint_source_file(routed, worktree, in_hint).or_else(|| {
            routing::first_source_file(
                &routed.effective_root,
                &routed.cfg.extensions,
                REPR_SCAN_DEPTH,
            )
        });
        if let Some(file) = file {
            let _ = instance.client.ensure_open(&file);
        }
    }

    /// The `in` hint resolved to an absolute, server-confined path when it names a
    /// file the routed server handles and that file exists; else `None`.
    fn hint_source_file(
        &self,
        routed: &RoutedServer,
        worktree: &Path,
        in_hint: Option<&Path>,
    ) -> Option<PathBuf> {
        let hint = in_hint?;
        let ext = hint.extension().and_then(|e| e.to_str())?;
        if !routed.cfg.extensions.iter().any(|e| e == ext) {
            return None;
        }
        let abs = if hint.is_absolute() {
            hint.to_path_buf()
        } else {
            worktree.join(hint)
        };
        let translated = routed.translate_file(&abs);
        translated.is_file().then_some(translated)
    }

    /// Background-prewarm `worktree`'s routed servers on a detached thread so
    /// indexing overlaps the agent's other work and never blocks a tool call.
    /// Idempotent via the pool (`get_or_spawn` reuses a live instance).
    pub fn lsp_prewarm_detached(&self, worktree: PathBuf) {
        let orch = self.clone();
        std::thread::spawn(move || orch.lsp_prewarm(&worktree));
    }

    /// Background-prewarm every project's main checkout so the shared
    /// main-keyed server is warm at boot. Best-effort. Canonicalize the repo
    /// path so the InstanceKey matches the root equivalent worktrees reroute
    /// onto (`base_checkout` canonicalizes its result).
    pub async fn lsp_prewarm_main_checkouts(&self) {
        let projects = match crate::projects::crud::list_db(&self.db.local).await {
            Ok(p) => p,
            Err(e) => {
                log::warn!("lsp boot prewarm: list projects failed: {e}");
                return;
            }
        };
        for project in projects {
            if project.repo_path.is_empty() {
                continue;
            }
            let path = std::path::Path::new(&project.repo_path);
            let canon = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
            self.lsp_prewarm_detached(canon);
        }
    }

    /// The dedicated, confined cache directory for one instance, under
    /// `{cairnHome}/lsp-cache/<hash of language+root>`.
    fn lsp_cache_dir(&self, key: &InstanceKey) -> PathBuf {
        let mut hasher = Sha256::new();
        hasher.update(key.language.as_bytes());
        hasher.update(b"\0");
        hasher.update(key.root.to_string_lossy().as_bytes());
        let hash = hasher.finalize();
        let hex: String = hash.iter().take(8).map(|b| format!("{b:02x}")).collect();
        self.config_dir.join("lsp-cache").join(hex)
    }

    /// Runtime status of every pooled language-server instance (Phase 4).
    pub fn lsp_instance_statuses(&self) -> Vec<LspInstanceStatus> {
        self.lsp_manager.statuses()
    }

    /// Sweep idle instances. Called from the warm-process eviction cadence.
    pub fn lsp_collect_idle(&self) {
        self.lsp_manager.collect_idle();
    }

    /// Stop every pooled language-server instance (shutdown).
    pub fn stop_lsp_services(&self) {
        self.lsp_manager.stop_all();
    }

    /// Runtime status of every configured (or default) language server, for the
    /// settings UI. Mirrors `build_service_statuses`: config plus the install
    /// signal (launch program on PATH) and the warm-instance view — the LSP
    /// analog of a build service's reachable daemon. Cheap: it reads settings
    /// and the already-pooled instance list, spawning nothing.
    pub fn language_server_statuses(&self) -> Vec<LanguageServerStatus> {
        let templates = settings::build_service_templates(&self.config_dir, None);
        let warm = self.lsp_manager.statuses();
        let mut out: Vec<LanguageServerStatus> = settings::load_language_servers(&self.config_dir)
            .into_iter()
            .map(|(name, cfg)| {
                let mut env_keys: Vec<String> = cfg.env.keys().cloned().collect();
                env_keys.sort();
                let instances = warm
                    .iter()
                    .filter(|s| s.language == name)
                    .cloned()
                    .collect();
                LanguageServerStatus {
                    enabled: cfg.enabled,
                    installed: language_server_on_path(&cfg),
                    command: cfg.expanded_command(&templates),
                    extensions: cfg.extensions.clone(),
                    root_markers: cfg.root_markers.clone(),
                    container_separator: cfg.container_separator.clone(),
                    env_keys,
                    instances,
                    name,
                    config: cfg,
                }
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }
}

/// Runtime status of one configured (or default) language server, for the
/// settings UI. Mirrors `BuildServiceStatus`: the raw editable config plus the
/// install signal and the warm-instance view.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LanguageServerStatus {
    /// The language id / registry key (e.g. `rust`, `typescript`).
    pub name: String,
    /// Whether Cairn launches this server (settings flag).
    pub enabled: bool,
    /// Whether the launch program resolves on PATH (or is an absolute path).
    pub installed: bool,
    /// The launch argv, templates expanded (for display).
    pub command: Vec<String>,
    /// File extensions routed to this server.
    pub extensions: Vec<String>,
    /// Files that mark the indexing root.
    pub root_markers: Vec<String>,
    /// Separator for container-qualified symbol names.
    pub container_separator: String,
    /// Sorted env keys injected into the server's spawn (values omitted).
    pub env_keys: Vec<String>,
    /// Currently pooled (warm) instances of this language server — the LSP
    /// analog of a build service's reachable daemon. Empty until a query (or
    /// pre-warm) has spawned one.
    pub instances: Vec<LspInstanceStatus>,
    /// The raw, template-unexpanded config — the editable source of truth the
    /// settings UI binds its form to (so edits round-trip `{cairnHome}` etc.).
    pub config: LanguageServerConfig,
}

/// Whether a language server's launch program resolves (on PATH or an absolute
/// path). The built-in defaults use this to report "not installed" until the
/// server binary is actually present.
fn language_server_on_path(cfg: &LanguageServerConfig) -> bool {
    match cfg.command.first() {
        Some(prog) => Path::new(prog).is_absolute() || crate::env::find_binary(prog).is_ok(),
        None => false,
    }
}

/// An honest, actionable message for a server that spawned but never completed
/// its `initialize` handshake (crashed, or isn't a real LSP server), including a
/// stderr tail when one was captured.
fn server_failure_message(language: &str, instance: &LspInstance) -> String {
    let mut message = format!(
        "`{language}` language server failed to start (it may not be installed, or it crashed during startup)"
    );
    if let Some(line) = instance
        .client
        .stderr_tail()
        .into_iter()
        .rev()
        .find(|line| !line.trim().is_empty())
    {
        message.push_str(&format!(" — last stderr: {line}"));
    }
    message
}

/// Render a clean miss, or — when no server answered but at least one could not
/// be reached — the honest failure list instead of an indistinguishable miss.
fn render_miss_or_failures(failures: Vec<String>) -> Rendered {
    if failures.is_empty() {
        render::render(&QueryOutcome::Miss, false)
    } else {
        Rendered {
            body: failures.join("\n"),
            suffix: None,
        }
    }
}

/// Compose an overview block: definition locations (grep rows) above the hover
/// signature/doc. Either part may be empty.
fn compose_overview(
    locations: &[LocationHit],
    hover: Option<&str>,
    still_indexing: bool,
) -> Rendered {
    let mut rendered = if locations.is_empty() {
        Rendered {
            body: String::new(),
            suffix: None,
        }
    } else {
        render::render(&QueryOutcome::Locations(locations.to_vec()), false)
    };
    if let Some(hover) = hover.filter(|text| !text.trim().is_empty()) {
        if !rendered.body.is_empty() {
            rendered.body.push_str("\n\n");
        }
        rendered.body.push_str(hover);
    }
    if rendered.body.is_empty() {
        rendered.body = "no results".to_string();
    }
    if still_indexing {
        rendered.body.push('\n');
        rendered
            .body
            .push_str("(server still indexing; results may be incomplete)");
    }
    rendered
}

/// Flatten one `publishDiagnostics` frame into grep-style location hits,
/// optionally filtered to paths under `filter`.
///
/// Diagnostic URIs are rooted at the server's `effective_root`, which may be the
/// main checkout when the worktree subroot was rerouted. Each path is mapped
/// back to the agent worktree (`worktree_root` mirrors `effective_root` byte for
/// byte) before filtering and display, so filters and rendered paths are always
/// worktree-relative. When not rerouted `effective_root == worktree_root` and
/// this is an identity map.
fn collect_diagnostic_hits(
    frame: &Value,
    worktree: &Path,
    effective_root: &Path,
    worktree_root: &Path,
    filter: Option<&Path>,
    hits: &mut Vec<LocationHit>,
) {
    let Some(uri) = frame.get("uri").and_then(|u| u.as_str()) else {
        return;
    };
    let path = crate::lsp::client::uri_to_path(uri);
    let worktree_path = path.as_ref().map(|p| match p.strip_prefix(effective_root) {
        Ok(rel) => worktree_root.join(rel),
        Err(_) => p.clone(),
    });
    if let (Some(filter), Some(path)) = (filter, worktree_path.as_ref()) {
        if !path.starts_with(filter) {
            return;
        }
    }
    let display = match worktree_path.as_ref() {
        Some(path) => path
            .strip_prefix(worktree)
            .unwrap_or(path)
            .to_string_lossy()
            .to_string(),
        None => uri.to_string(),
    };
    let Some(diagnostics) = frame.get("diagnostics").and_then(|d| d.as_array()) else {
        return;
    };
    for diagnostic in diagnostics {
        let line = diagnostic
            .pointer("/range/start/line")
            .and_then(|l| l.as_u64())
            .unwrap_or(0) as u32;
        let message = diagnostic
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .replace('\n', " ");
        let severity = diagnostic
            .get("severity")
            .and_then(|s| s.as_u64())
            .map(severity_label)
            .unwrap_or("");
        let snippet = if severity.is_empty() {
            message
        } else {
            format!("{severity}: {message}")
        };
        hits.push(LocationHit {
            path: display.clone(),
            line: line + 1,
            snippet,
        });
    }
}

fn severity_label(severity: u64) -> &'static str {
    match severity {
        1 => "error",
        2 => "warning",
        3 => "information",
        4 => "hint",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::services::{GitClient, RealGitClient};
    use crate::storage::{LocalDb, SearchIndex};
    use std::fs;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// A minimal real-services Orchestrator: a migrated DB plus a `RealGitClient`
    /// so the reroute decision (`resolve_effective_root`/`base_checkout`) runs
    /// against actual git worktrees. The DB is unused by these tests but the
    /// builder requires it.
    async fn test_orchestrator() -> Orchestrator {
        let db: LocalDb = crate::storage::migrated_test_db("lsp-reroute-test.db").await;
        let temp = tempdir().unwrap();
        let config_dir = temp.keep();
        let index_path = config_dir.join("search-index.db");
        let db_state = Arc::new(DbState::new(
            Arc::new(db),
            Arc::new(SearchIndex::open_or_create(index_path).unwrap()),
        ));
        let services = Arc::new(TestServicesBuilder::new().with_git(RealGitClient).build());
        Orchestrator::builder(db_state, services, config_dir).build()
    }

    /// Initialize a main checkout with a `crate/` subroot and one commit.
    fn init_repo(root: &Path) -> RealGitClient {
        let git = RealGitClient;
        git.init_repo(root, "main").unwrap();
        fs::create_dir_all(root.join("crate/src")).unwrap();
        fs::write(root.join("crate/Cargo.toml"), "[package]\nname=\"x\"\n").unwrap();
        fs::write(root.join("crate/src/lib.rs"), "pub fn a() {}\n").unwrap();
        git.add_all(root).unwrap();
        git.commit(root, "init").unwrap();
        git
    }

    #[tokio::test(flavor = "current_thread")]
    async fn equivalent_worktrees_collapse_onto_one_base_root() {
        let orch = test_orchestrator().await;
        let main_dir = tempdir().unwrap();
        let main = main_dir.path();
        let git = init_repo(main);
        let wt_root = tempdir().unwrap();
        let wt_a = wt_root.path().join("a");
        let wt_b = wt_root.path().join("b");
        git.worktree_add_new_branch(main, &wt_a, "feat-a", "main")
            .unwrap();
        git.worktree_add_new_branch(main, &wt_b, "feat-b", "main")
            .unwrap();

        let (eff_a, rr_a) = orch.resolve_effective_root(&wt_a, &wt_a.join("crate"));
        let (eff_b, rr_b) = orch.resolve_effective_root(&wt_b, &wt_b.join("crate"));

        // The rerouted root is the canonical main-checkout subroot.
        let base_crate = std::fs::canonicalize(main).unwrap().join("crate");
        assert!(rr_a && rr_b, "clean equivalent worktrees reroute to base");
        assert_eq!(eff_a, base_crate);
        assert_eq!(eff_b, base_crate);
        // Same effective root => same InstanceKey => one pooled instance.
        assert_eq!(
            InstanceKey::new("rust", eff_a),
            InstanceKey::new("rust", eff_b),
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn diverged_worktree_keeps_its_own_root() {
        let orch = test_orchestrator().await;
        let main_dir = tempdir().unwrap();
        let main = main_dir.path();
        let git = init_repo(main);
        let wt_root = tempdir().unwrap();
        let wt = wt_root.path().join("w");
        git.worktree_add_new_branch(main, &wt, "feat", "main")
            .unwrap();

        // A dirty (uncommitted) edit in the worktree subroot blocks the reroute.
        fs::write(wt.join("crate/src/lib.rs"), "pub fn a() { let _ = 1; }\n").unwrap();
        let sub = wt.join("crate");
        let (eff, rr) = orch.resolve_effective_root(&wt, &sub);
        assert!(!rr, "a dirty subroot must not reroute");
        assert_eq!(eff, sub);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn main_checkout_query_is_not_rerouted() {
        let orch = test_orchestrator().await;
        let main_dir = tempdir().unwrap();
        let main = main_dir.path();
        let _git = init_repo(main);
        // The repo is its own checkout: there is nothing to reroute to.
        assert!(orch.base_checkout(main).is_none());
        let sub = main.join("crate");
        let (eff, rr) = orch.resolve_effective_root(main, &sub);
        assert!(!rr);
        assert_eq!(eff, sub);
    }

    #[test]
    fn diagnostic_hits_map_base_paths_back_to_worktree() {
        // A diagnostic emitted by a rerouted (base-rooted) server is mapped back
        // to the agent worktree's relative path.
        let worktree = Path::new("/wt/agent");
        let worktree_root = Path::new("/wt/agent/crate");
        let effective_root = Path::new("/repo/crate");
        let frame = serde_json::json!({
            "uri": "file:///repo/crate/src/lib.rs",
            "diagnostics": [{
                "range": {"start": {"line": 4, "character": 0}},
                "message": "unused variable",
                "severity": 2
            }]
        });
        let mut hits = Vec::new();
        collect_diagnostic_hits(
            &frame,
            worktree,
            effective_root,
            worktree_root,
            None,
            &mut hits,
        );
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].path, "crate/src/lib.rs");
        assert_eq!(hits[0].line, 5);
        assert_eq!(hits[0].snippet, "warning: unused variable");

        // A worktree-relative filter is applied against the mapped path.
        let mut kept = Vec::new();
        collect_diagnostic_hits(
            &frame,
            worktree,
            effective_root,
            worktree_root,
            Some(Path::new("/wt/agent/crate")),
            &mut kept,
        );
        assert_eq!(kept.len(), 1);
        let mut excluded = Vec::new();
        collect_diagnostic_hits(
            &frame,
            worktree,
            effective_root,
            worktree_root,
            Some(Path::new("/wt/agent/other")),
            &mut excluded,
        );
        assert!(excluded.is_empty());
    }
}
