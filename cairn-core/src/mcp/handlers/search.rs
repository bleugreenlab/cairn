//! Search MCP handlers.
//!
//! Handles: search, glob, grep

use super::{lookup_project_context, lookup_run};
use crate::config::project_settings::load_project_settings;
use crate::mcp::types::McpCallbackRequest;
use crate::models::{SearchContentType, SearchFilters};
use crate::orchestrator::Orchestrator;
use crate::resources::resolve_resource_path;
use crate::schema::projects;
use diesel::prelude::*;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Payload for search tool
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchPayload {
    pub query: String,
    pub content_types: Option<Vec<String>>,
    pub project_id: Option<String>,
    pub issue_id: Option<String>,
    pub since: Option<i64>,
    pub limit: Option<usize>,
}

/// Handle search tool call - searches across issues, comments, artifacts, and events.
pub async fn handle_search(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: SearchPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    log::info!("search called: query={}", payload.query);

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    // Get project context via run chain (internal agents), or none (external callers)
    let ctx = lookup_project_context(&mut conn, request).ok();

    // Build filters - use provided project_id or default to current project from run chain
    let project_id = payload
        .project_id
        .or_else(|| ctx.as_ref().map(|c| c.project_id.clone()));

    let filters = SearchFilters {
        project_id,
        issue_id: payload.issue_id,
        content_types: payload.content_types,
        since: payload.since,
        limit: payload.limit,
    };

    match crate::search::search_content(&mut conn, &payload.query, Some(filters)) {
        Ok(results) => {
            format_search_results(&results, ctx.as_ref().map(|c| c.project_key.as_str()))
        }
        Err(e) => format!("Search failed: {}", e),
    }
}

/// Format search results as human-readable text for the agent.
fn format_search_results(
    results: &[crate::models::SearchResult],
    project_key: Option<&str>,
) -> String {
    if results.is_empty() {
        return "No results found.".to_string();
    }

    let mut output = format!("Found {} result(s):\n\n", results.len());

    for (i, result) in results.iter().enumerate() {
        let type_label = match result.content_type {
            SearchContentType::Issue => "Issue",
            SearchContentType::Comment => "Comment",
            SearchContentType::Artifact => "Artifact",
            SearchContentType::Event => "Event",
            SearchContentType::Message => "Message",
        };

        // Use the URI from the search result directly
        let uri = if result.uri.is_empty() {
            let key = project_key.unwrap_or("PROJECT");
            format!("cairn://{}/{}", key, result.id)
        } else {
            result.uri.clone()
        };

        output.push_str(&format!("{}. [{}] {}\n", i + 1, type_label, result.title));
        output.push_str(&format!("   URI: {}\n", uri));
        output.push_str(&format!("   {}\n\n", result.snippet));
    }

    output
}

// ---------------------------------------------------------------------------
// Glob / Grep handlers
// ---------------------------------------------------------------------------

const WALK_TIMEOUT: Duration = Duration::from_secs(30);
const GREP_TIMEOUT: Duration = Duration::from_secs(30);

/// Payload for glob tool (matches cairn-mcp GlobInput)
#[derive(Debug, Clone, Deserialize)]
pub struct GlobPayload {
    pub pattern: String,
    pub path: Option<String>,
}

/// Payload for grep tool (matches cairn-mcp GrepInput)
#[derive(Debug, Clone, Deserialize)]
pub struct GrepPayload {
    pub pattern: String,
    pub path: Option<String>,
    pub glob: Option<String>,
    #[serde(rename = "type")]
    pub file_type: Option<String>,
    pub output_mode: Option<String>,
    pub context: Option<u32>,
    #[serde(rename = "-A")]
    pub after_context: Option<u32>,
    #[serde(rename = "-B")]
    pub before_context: Option<u32>,
    #[serde(rename = "-C")]
    pub context_alias: Option<u32>,
    #[serde(rename = "-i")]
    pub case_insensitive: Option<bool>,
    #[serde(rename = "-n")]
    pub line_numbers: Option<bool>,
    pub head_limit: Option<u32>,
    pub offset: Option<u32>,
    pub multiline: Option<bool>,
}

/// Build the set of directories that a search tool is allowed to access.
///
/// Always includes the agent's cwd. If we can look up the run's project,
/// also includes any configured resource directories.
fn resolve_allowed_directories(orch: &Orchestrator, request: &McpCallbackRequest) -> Vec<PathBuf> {
    let mut allowed = vec![PathBuf::from(&request.cwd)];

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(_) => return allowed,
    };

    if let Ok(ctx) = lookup_run(&mut conn, request) {
        // Get the project repo_path so we can load project settings
        let repo_path: Option<String> = projects::table
            .find(&ctx.project_id)
            .select(projects::repo_path)
            .first(&mut *conn)
            .ok();

        if let Some(ref path) = repo_path {
            let settings = load_project_settings(Path::new(path));
            if let Some(resources) = settings.resources {
                for resource in &resources {
                    if let Some(resolved) = resolve_resource_path(&orch.config_dir, resource) {
                        allowed.push(resolved);
                    }
                }
            }
        }
    }

    allowed
}

/// Check that `search_path` is inside one of the `allowed` directories.
fn validate_search_path(search_path: &Path, allowed: &[PathBuf]) -> Result<(), String> {
    let canonical = search_path
        .canonicalize()
        .map_err(|e| format!("Cannot resolve path '{}': {}", search_path.display(), e))?;

    for dir in allowed {
        if let Ok(allowed_canonical) = dir.canonicalize() {
            if canonical.starts_with(&allowed_canonical) {
                return Ok(());
            }
        }
    }

    Err(format!(
        "Path '{}' is outside allowed directories. Allowed: {:?}",
        search_path.display(),
        allowed
            .iter()
            .map(|d| d.display().to_string())
            .collect::<Vec<_>>()
    ))
}

/// Resolve the search directory from the payload path and the agent cwd.
fn resolve_search_dir(cwd: &str, path: Option<&str>) -> PathBuf {
    match path {
        Some(p) => {
            let p = Path::new(p);
            if p.is_absolute() {
                p.to_path_buf()
            } else {
                Path::new(cwd).join(p)
            }
        }
        None => PathBuf::from(cwd),
    }
}

/// Handle glob tool call — pattern-match files with path scoping.
pub fn handle_glob(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: GlobPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    let search_dir = resolve_search_dir(&request.cwd, payload.path.as_deref());
    log::info!(
        "glob called: pattern={}, dir={}",
        payload.pattern,
        search_dir.display()
    );

    // Path scoping
    let allowed = resolve_allowed_directories(orch, request);
    if let Err(e) = validate_search_path(&search_dir, &allowed) {
        return e;
    }

    // Build glob matcher
    let glob = match globset::GlobBuilder::new(&payload.pattern)
        .literal_separator(false)
        .build()
    {
        Ok(g) => g,
        Err(e) => {
            return format!("Invalid glob pattern '{}': {}", payload.pattern, e);
        }
    };
    let matcher = match globset::GlobSetBuilder::new().add(glob).build() {
        Ok(m) => m,
        Err(e) => {
            return format!("Failed to build glob matcher: {}", e);
        }
    };

    // Walk directory respecting .gitignore, with timeout
    let walker = ignore::WalkBuilder::new(&search_dir)
        .hidden(false)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true)
        .build();

    let deadline = std::time::Instant::now() + WALK_TIMEOUT;
    let mut matches: Vec<(PathBuf, std::time::SystemTime)> = Vec::new();
    let mut timed_out = false;

    for entry in walker {
        if std::time::Instant::now() > deadline {
            log::warn!("glob walk timed out after {:?}", WALK_TIMEOUT);
            timed_out = true;
            break;
        }
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        if entry.file_type().is_none_or(|ft| ft.is_dir()) {
            continue;
        }
        let path = entry.path();
        let relative = path.strip_prefix(&search_dir).unwrap_or(path);
        if matcher.is_match(relative) {
            let mtime = entry
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .unwrap_or(std::time::UNIX_EPOCH);
            matches.push((relative.to_path_buf(), mtime));
        }
    }

    // Sort by modification time, most recent first
    matches.sort_by(|a, b| b.1.cmp(&a.1));

    if matches.is_empty() && !timed_out {
        return format!(
            "No files matched pattern '{}' in {}",
            payload.pattern,
            search_dir.display()
        );
    }

    let mut result: String = matches
        .iter()
        .map(|(p, _)| p.display().to_string())
        .collect::<Vec<_>>()
        .join("\n");

    if timed_out {
        result.push_str(&format!(
            "\n\n[WARNING: Search timed out after {}s — results are incomplete. \
             Try a more specific path or pattern to narrow the search.]",
            WALK_TIMEOUT.as_secs()
        ));
    }

    result
}

/// Handle grep tool call — in-process ripgrep search with path scoping.
pub async fn handle_grep(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: GrepPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    let search_path = resolve_search_dir(&request.cwd, payload.path.as_deref());
    log::info!(
        "grep called: pattern={}, path={}",
        payload.pattern,
        search_path.display()
    );

    // Path scoping
    let allowed = resolve_allowed_directories(orch, request);
    if let Err(e) = validate_search_path(&search_path, &allowed) {
        return e;
    }

    // Validate output mode early
    let output_mode = payload
        .output_mode
        .as_deref()
        .unwrap_or("files_with_matches");
    if !matches!(output_mode, "files_with_matches" | "count" | "content") {
        return format!(
            "Invalid output_mode '{}'. Must be 'content', 'files_with_matches', or 'count'.",
            output_mode
        );
    }

    let output_mode = output_mode.to_string();
    let show_line_numbers = payload.line_numbers.unwrap_or(true);
    let offset = payload.offset.unwrap_or(0) as usize;
    let head_limit = payload.head_limit;

    let result = tokio::time::timeout(
        GREP_TIMEOUT,
        tokio::task::spawn_blocking(move || {
            grep_search(payload, &search_path, &output_mode, show_line_numbers)
        }),
    )
    .await;

    match result {
        Ok(Ok(Ok(output))) => {
            if output.is_empty() {
                return format!(
                    "No matches found for pattern '{}'",
                    &request.payload["pattern"].as_str().unwrap_or("?")
                );
            }

            let lines: Vec<&str> = output.lines().collect();
            let sliced = if offset >= lines.len() {
                Vec::new()
            } else {
                match head_limit {
                    Some(limit) => lines[offset..]
                        .iter()
                        .take(limit as usize)
                        .copied()
                        .collect(),
                    None => lines[offset..].to_vec(),
                }
            };

            sliced.join("\n")
        }
        Ok(Ok(Err(e))) => e,
        Ok(Err(e)) => format!("grep task failed: {}", e),
        Err(_) => format!("grep timed out after {:?}", GREP_TIMEOUT),
    }
}

/// Perform the actual in-process grep search.
fn grep_search(
    payload: GrepPayload,
    search_path: &Path,
    output_mode: &str,
    show_line_numbers: bool,
) -> Result<String, String> {
    use grep_regex::RegexMatcherBuilder;
    use grep_searcher::{BinaryDetection, Searcher, SearcherBuilder};
    use ignore::overrides::OverrideBuilder;
    use ignore::types::TypesBuilder;
    use ignore::WalkBuilder;

    let mut matcher_builder = RegexMatcherBuilder::new();
    if payload.case_insensitive.unwrap_or(false) {
        matcher_builder.case_insensitive(true);
    }
    if payload.multiline.unwrap_or(false) {
        matcher_builder.multi_line(true);
        matcher_builder.dot_matches_new_line(true);
    }
    let matcher = matcher_builder
        .build(&payload.pattern)
        .map_err(|e| format!("Invalid regex pattern '{}': {}", payload.pattern, e))?;

    let is_file = search_path.is_file();
    let walk_root = if is_file {
        search_path.parent().unwrap_or(search_path)
    } else {
        search_path
    };

    let mut walker_builder = WalkBuilder::new(if is_file { search_path } else { walk_root });
    walker_builder
        .hidden(false)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true);

    if let Some(ref ft) = payload.file_type {
        let mut types = TypesBuilder::new();
        types.add_defaults();
        types
            .select(ft)
            .build()
            .map_err(|e| format!("Invalid file type '{}': {}", ft, e))
            .map(|t| {
                walker_builder.types(t);
            })?;
    }

    if let Some(ref g) = payload.glob {
        let mut overrides = OverrideBuilder::new(walk_root);
        overrides
            .add(g)
            .map_err(|e| format!("Invalid glob '{}': {}", g, e))?;
        walker_builder.overrides(
            overrides
                .build()
                .map_err(|e| format!("Failed to build glob override: {}", e))?,
        );
    }

    let mut searcher_builder = SearcherBuilder::new();
    searcher_builder
        .binary_detection(BinaryDetection::quit(b'\x00'))
        .line_number(true);

    let context = payload.context_alias.or(payload.context);
    if let Some(c) = context {
        searcher_builder
            .before_context(c as usize)
            .after_context(c as usize);
    }
    if let Some(a) = payload.after_context {
        searcher_builder.after_context(a as usize);
    }
    if let Some(b) = payload.before_context {
        searcher_builder.before_context(b as usize);
    }
    if payload.multiline.unwrap_or(false) {
        searcher_builder.multi_line(true);
    }

    let mut searcher = searcher_builder.build();

    let base_path = if is_file {
        search_path.parent().unwrap_or(search_path)
    } else {
        search_path
    };

    let mut output_lines: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + GREP_TIMEOUT;

    for entry in walker_builder.build() {
        if std::time::Instant::now() > deadline {
            log::warn!("grep walk timed out");
            break;
        }

        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };

        if entry.file_type().is_none_or(|ft| ft.is_dir()) {
            continue;
        }

        let path = entry.path().to_path_buf();
        let relative = path
            .strip_prefix(base_path)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();

        match output_mode {
            "files_with_matches" => {
                let mut found = false;
                let sink = grep_searcher::sinks::UTF8(|_, _| {
                    found = true;
                    Ok(false)
                });
                let _ = searcher.search_path(&matcher, &path, sink);
                if found {
                    output_lines.push(relative);
                }
            }
            "count" => {
                let mut count: u64 = 0;
                let sink = grep_searcher::sinks::UTF8(|_, _| {
                    count += 1;
                    Ok(true)
                });
                let _ = searcher.search_path(&matcher, &path, sink);
                if count > 0 {
                    output_lines.push(format!("{}:{}", relative, count));
                }
            }
            "content" => {
                struct ContentSink {
                    relative: String,
                    show_line_numbers: bool,
                    lines: Vec<String>,
                    needs_separator: bool,
                }

                impl grep_searcher::Sink for ContentSink {
                    type Error = std::io::Error;

                    fn matched(
                        &mut self,
                        _searcher: &Searcher,
                        mat: &grep_searcher::SinkMatch<'_>,
                    ) -> Result<bool, Self::Error> {
                        self.needs_separator = true;
                        let text = std::str::from_utf8(mat.bytes())
                            .unwrap_or("")
                            .trim_end_matches('\n')
                            .trim_end_matches('\r');
                        if self.show_line_numbers {
                            if let Some(n) = mat.line_number() {
                                self.lines.push(format!("{}:{}:{}", self.relative, n, text));
                            } else {
                                self.lines.push(format!("{}:{}", self.relative, text));
                            }
                        } else {
                            self.lines.push(format!("{}:{}", self.relative, text));
                        }
                        Ok(true)
                    }

                    fn context(
                        &mut self,
                        _searcher: &Searcher,
                        ctx: &grep_searcher::SinkContext<'_>,
                    ) -> Result<bool, Self::Error> {
                        let text = std::str::from_utf8(ctx.bytes())
                            .unwrap_or("")
                            .trim_end_matches('\n')
                            .trim_end_matches('\r');
                        if self.show_line_numbers {
                            if let Some(n) = ctx.line_number() {
                                self.lines.push(format!("{}:{}-{}", self.relative, n, text));
                            } else {
                                self.lines.push(format!("{}-{}", self.relative, text));
                            }
                        } else {
                            self.lines.push(format!("{}-{}", self.relative, text));
                        }
                        Ok(true)
                    }

                    fn context_break(&mut self, _searcher: &Searcher) -> Result<bool, Self::Error> {
                        if self.needs_separator {
                            self.lines.push("--".to_string());
                            self.needs_separator = false;
                        }
                        Ok(true)
                    }
                }

                let mut sink = ContentSink {
                    relative: relative.clone(),
                    show_line_numbers,
                    lines: Vec::new(),
                    needs_separator: false,
                };
                let _ = searcher.search_path(&matcher, &path, &mut sink);
                if sink.lines.last().is_some_and(|l| l == "--") {
                    sink.lines.pop();
                }
                output_lines.extend(sink.lines);
            }
            _ => unreachable!(),
        }
    }

    Ok(output_lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::test_utils::test_diesel_conn;
    use std::fs;
    use std::sync::{Arc, Mutex};

    fn test_orchestrator() -> Orchestrator {
        let conn = test_diesel_conn();
        let db = Arc::new(DbState {
            conn: Mutex::new(conn),
        });
        let services = Arc::new(TestServicesBuilder::new().build());
        let account_manager = Arc::new(crate::orchestrator::AccountManager::new(
            db.clone(),
            services.emitter.clone(),
        ));
        let sync_tx = Arc::new(Mutex::new(None));
        Orchestrator {
            db,
            services: services.clone(),
            process_state: Arc::new(crate::agent_process::process::AgentProcessState::default()),
            mcp_auth: Arc::new(crate::mcp::McpAuthState::new(PathBuf::from("/tmp"))),
            warm_gc: None,
            pty_state: Arc::new(crate::services::PtyState::default()),
            permission_responses: tokio::sync::broadcast::channel(16).0,
            run_completions: tokio::sync::broadcast::channel(64).0,
            prompt_responses: tokio::sync::broadcast::channel(16).0,
            trigger_events: tokio::sync::broadcast::channel(256).0,
            session_allowed_tools: Arc::new(Mutex::new(std::collections::HashSet::new())),
            identity_store: Arc::new(Mutex::new(None)),
            mcp_binary_path: "cairn-mcp".to_string(),
            config_dir: PathBuf::from("/tmp"),
            schema_dir: None,
            mcp_callback_port: 3847,
            embedding_engine: None,
            vibe_state: None,
            account_manager,
            sync_tx: sync_tx.clone(),
            notifier: crate::notify::Notifier::new(sync_tx, services.emitter.clone()),
            api_config: crate::api::ApiConfig::default(),
            effect_tx: None,
            model_catalog: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            provider_usage_snapshots: Default::default(),
            executor: std::sync::Arc::new(std::sync::OnceLock::new()),
        }
    }

    #[test]
    fn validate_search_path_within_allowed_dir() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir_all(&sub).unwrap();

        let allowed = vec![dir.path().to_path_buf()];
        assert!(validate_search_path(&sub, &allowed).is_ok());
    }

    #[test]
    fn validate_search_path_rejects_outside_dir() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();

        let allowed = vec![dir1.path().to_path_buf()];
        let result = validate_search_path(dir2.path(), &allowed);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("outside allowed directories"));
    }

    #[test]
    fn validate_search_path_exact_match_allowed() {
        let dir = tempfile::tempdir().unwrap();
        let allowed = vec![dir.path().to_path_buf()];
        assert!(validate_search_path(dir.path(), &allowed).is_ok());
    }

    #[test]
    fn validate_search_path_rejects_nonexistent() {
        let allowed = vec![PathBuf::from("/tmp")];
        let result = validate_search_path(Path::new("/nonexistent/path/xyz"), &allowed);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Cannot resolve path"));
    }

    #[test]
    fn validate_search_path_multiple_allowed_dirs() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let sub2 = dir2.path().join("inner");
        fs::create_dir_all(&sub2).unwrap();

        let allowed = vec![dir1.path().to_path_buf(), dir2.path().to_path_buf()];
        assert!(validate_search_path(&sub2, &allowed).is_ok());
    }

    #[test]
    fn validate_search_path_rejects_symlink_escape() {
        let allowed_dir = tempfile::tempdir().unwrap();
        let outside_dir = tempfile::tempdir().unwrap();

        let link = allowed_dir.path().join("escape");
        #[cfg(unix)]
        std::os::unix::fs::symlink(outside_dir.path(), &link).unwrap();

        let allowed = vec![allowed_dir.path().to_path_buf()];
        let result = validate_search_path(&link, &allowed);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_search_dir_absolute_path() {
        let result = resolve_search_dir("/some/cwd", Some("/absolute/path"));
        assert_eq!(result, PathBuf::from("/absolute/path"));
    }

    #[test]
    fn resolve_search_dir_relative_path() {
        let result = resolve_search_dir("/some/cwd", Some("relative/sub"));
        assert_eq!(result, PathBuf::from("/some/cwd/relative/sub"));
    }

    #[test]
    fn resolve_search_dir_none_uses_cwd() {
        let result = resolve_search_dir("/some/cwd", None);
        assert_eq!(result, PathBuf::from("/some/cwd"));
    }

    #[test]
    fn handle_glob_within_cwd() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("foo.rs"), "fn main() {}").unwrap();
        fs::write(dir.path().join("bar.txt"), "hello").unwrap();

        let orch = test_orchestrator();

        let request = McpCallbackRequest {
            cwd: dir.path().to_string_lossy().to_string(),
            run_id: None,
            tool: "glob".to_string(),
            payload: serde_json::json!({ "pattern": "*.rs" }),
            tool_use_id: None,
        };

        let result = handle_glob(&orch, &request);
        assert!(result.contains("foo.rs"), "Expected foo.rs in: {}", result);
        assert!(!result.contains("bar.txt"));
    }

    #[test]
    fn handle_glob_outside_cwd_rejected() {
        let cwd = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("secret.txt"), "data").unwrap();

        let orch = test_orchestrator();

        let request = McpCallbackRequest {
            cwd: cwd.path().to_string_lossy().to_string(),
            run_id: None,
            tool: "glob".to_string(),
            payload: serde_json::json!({
                "pattern": "*.txt",
                "path": outside.path().to_string_lossy().to_string()
            }),
            tool_use_id: None,
        };

        let result = handle_glob(&orch, &request);
        assert!(
            result.contains("outside allowed directories"),
            "Expected rejection, got: {}",
            result
        );
    }

    #[test]
    fn handle_glob_invalid_pattern() {
        let dir = tempfile::tempdir().unwrap();
        let orch = test_orchestrator();

        let request = McpCallbackRequest {
            cwd: dir.path().to_string_lossy().to_string(),
            run_id: None,
            tool: "glob".to_string(),
            payload: serde_json::json!({ "pattern": "[invalid" }),
            tool_use_id: None,
        };

        let result = handle_glob(&orch, &request);
        assert!(
            result.contains("Invalid glob pattern"),
            "Expected error, got: {}",
            result
        );
    }

    #[test]
    fn handle_glob_no_matches() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("foo.txt"), "hello").unwrap();

        let orch = test_orchestrator();

        let request = McpCallbackRequest {
            cwd: dir.path().to_string_lossy().to_string(),
            run_id: None,
            tool: "glob".to_string(),
            payload: serde_json::json!({ "pattern": "*.rs" }),
            tool_use_id: None,
        };

        let result = handle_glob(&orch, &request);
        assert!(
            result.contains("No files matched"),
            "Expected no match message, got: {}",
            result
        );
    }

    #[test]
    fn handle_glob_sorts_by_mtime_most_recent_first() {
        let dir = tempfile::tempdir().unwrap();

        // Create files with a time gap to ensure different mtimes
        fs::write(dir.path().join("old.rs"), "old").unwrap();
        // Set mtime to 10 seconds ago
        let old_time = filetime::FileTime::from_unix_time(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                - 10,
            0,
        );
        filetime::set_file_mtime(dir.path().join("old.rs"), old_time).unwrap();

        fs::write(dir.path().join("new.rs"), "new").unwrap();

        let orch = test_orchestrator();
        let request = McpCallbackRequest {
            cwd: dir.path().to_string_lossy().to_string(),
            run_id: None,
            tool: "glob".to_string(),
            payload: serde_json::json!({ "pattern": "*.rs" }),
            tool_use_id: None,
        };

        let result = handle_glob(&orch, &request);
        let lines: Vec<&str> = result.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "new.rs", "Most recent file should be first");
        assert_eq!(lines[1], "old.rs");
    }

    #[test]
    fn handle_glob_invalid_payload() {
        let dir = tempfile::tempdir().unwrap();
        let orch = test_orchestrator();

        let request = McpCallbackRequest {
            cwd: dir.path().to_string_lossy().to_string(),
            run_id: None,
            tool: "glob".to_string(),
            payload: serde_json::json!({ "wrong_field": true }),
            tool_use_id: None,
        };

        let result = handle_glob(&orch, &request);
        assert!(
            result.contains("Invalid payload"),
            "Expected invalid payload error, got: {}",
            result
        );
    }

    // -----------------------------------------------------------------------
    // handle_grep tests
    // -----------------------------------------------------------------------

    fn grep_request(cwd: &str, payload: serde_json::Value) -> McpCallbackRequest {
        McpCallbackRequest {
            cwd: cwd.to_string(),
            run_id: None,
            tool: "grep".to_string(),
            payload,
            tool_use_id: None,
        }
    }

    #[tokio::test]
    async fn handle_grep_finds_matching_content() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("hello.txt"), "hello world\ngoodbye world").unwrap();

        let orch = test_orchestrator();
        let request = grep_request(
            &dir.path().to_string_lossy(),
            serde_json::json!({
                "pattern": "hello",
                "output_mode": "content"
            }),
        );

        let result = handle_grep(&orch, &request).await;
        assert!(
            result.contains("hello world"),
            "Expected match, got: {}",
            result
        );
        assert!(!result.contains("goodbye"));
    }

    #[tokio::test]
    async fn handle_grep_files_with_matches_mode() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "needle here").unwrap();
        fs::write(dir.path().join("b.txt"), "no match").unwrap();

        let orch = test_orchestrator();
        let request = grep_request(
            &dir.path().to_string_lossy(),
            serde_json::json!({
                "pattern": "needle",
                "output_mode": "files_with_matches"
            }),
        );

        let result = handle_grep(&orch, &request).await;
        assert!(result.contains("a.txt"), "Expected a.txt, got: {}", result);
        assert!(!result.contains("b.txt"));
    }

    #[tokio::test]
    async fn handle_grep_count_mode() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("data.txt"), "foo\nfoo\nbar").unwrap();

        let orch = test_orchestrator();
        let request = grep_request(
            &dir.path().to_string_lossy(),
            serde_json::json!({
                "pattern": "foo",
                "output_mode": "count"
            }),
        );

        let result = handle_grep(&orch, &request).await;
        // rg --count outputs "file:count"
        assert!(
            result.contains(":2"),
            "Expected count of 2, got: {}",
            result
        );
    }

    #[tokio::test]
    async fn handle_grep_invalid_output_mode() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("x.txt"), "data").unwrap();

        let orch = test_orchestrator();
        let request = grep_request(
            &dir.path().to_string_lossy(),
            serde_json::json!({
                "pattern": "data",
                "output_mode": "bogus"
            }),
        );

        let result = handle_grep(&orch, &request).await;
        assert!(
            result.contains("Invalid output_mode"),
            "Expected output_mode error, got: {}",
            result
        );
    }

    #[tokio::test]
    async fn handle_grep_no_matches() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("data.txt"), "hello").unwrap();

        let orch = test_orchestrator();
        let request = grep_request(
            &dir.path().to_string_lossy(),
            serde_json::json!({ "pattern": "zzzznothere" }),
        );

        let result = handle_grep(&orch, &request).await;
        assert!(
            result.contains("No matches found"),
            "Expected no matches, got: {}",
            result
        );
    }

    #[tokio::test]
    async fn handle_grep_outside_cwd_rejected() {
        let cwd = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("secret.txt"), "password").unwrap();

        let orch = test_orchestrator();
        let request = grep_request(
            &cwd.path().to_string_lossy(),
            serde_json::json!({
                "pattern": "password",
                "path": outside.path().to_string_lossy().to_string()
            }),
        );

        let result = handle_grep(&orch, &request).await;
        assert!(
            result.contains("outside allowed directories"),
            "Expected rejection, got: {}",
            result
        );
    }

    #[tokio::test]
    async fn handle_grep_case_insensitive() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("data.txt"), "Hello World").unwrap();

        let orch = test_orchestrator();
        let request = grep_request(
            &dir.path().to_string_lossy(),
            serde_json::json!({
                "pattern": "hello",
                "-i": true,
                "output_mode": "content"
            }),
        );

        let result = handle_grep(&orch, &request).await;
        assert!(
            result.contains("Hello World"),
            "Expected case-insensitive match, got: {}",
            result
        );
    }

    #[tokio::test]
    async fn handle_grep_head_limit_and_offset() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("lines.txt"),
            "line1 match\nline2 match\nline3 match\nline4 match\nline5 match",
        )
        .unwrap();

        let orch = test_orchestrator();
        let request = grep_request(
            &dir.path().to_string_lossy(),
            serde_json::json!({
                "pattern": "match",
                "output_mode": "content",
                "offset": 1,
                "head_limit": 2,
                "-n": false
            }),
        );

        let result = handle_grep(&orch, &request).await;
        let lines: Vec<&str> = result.lines().collect();
        assert_eq!(
            lines.len(),
            2,
            "Expected 2 lines after offset+limit, got: {:?}",
            lines
        );
    }

    #[tokio::test]
    async fn handle_grep_invalid_payload() {
        let dir = tempfile::tempdir().unwrap();
        let orch = test_orchestrator();

        let request = grep_request(
            &dir.path().to_string_lossy(),
            serde_json::json!({ "not_pattern": "x" }),
        );

        let result = handle_grep(&orch, &request).await;
        assert!(
            result.contains("Invalid payload"),
            "Expected invalid payload error, got: {}",
            result
        );
    }

    #[tokio::test]
    async fn handle_grep_glob_filter() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("code.rs"), "fn main() {}").unwrap();
        fs::write(dir.path().join("readme.md"), "fn main() {}").unwrap();

        let orch = test_orchestrator();
        let request = grep_request(
            &dir.path().to_string_lossy(),
            serde_json::json!({
                "pattern": "fn main",
                "glob": "*.rs",
                "output_mode": "files_with_matches"
            }),
        );

        let result = handle_grep(&orch, &request).await;
        assert!(
            result.contains("code.rs"),
            "Expected code.rs, got: {}",
            result
        );
        assert!(!result.contains("readme.md"));
    }

    #[tokio::test]
    async fn handle_grep_default_mode_is_files_with_matches() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("f.txt"), "target_data").unwrap();

        let orch = test_orchestrator();
        // No output_mode specified — should default to files_with_matches
        let request = grep_request(
            &dir.path().to_string_lossy(),
            serde_json::json!({ "pattern": "target_data" }),
        );

        let result = handle_grep(&orch, &request).await;
        // files_with_matches returns filenames, not content lines
        assert!(
            result.contains("f.txt"),
            "Expected filename in result, got: {}",
            result
        );
        // Should not contain the actual matched content (just the filename)
        assert!(
            !result.contains("target_data"),
            "Expected only filename, not content, got: {}",
            result
        );
    }
}
